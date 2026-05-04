from typing import Literal
from load.kafka_consumer import KafkaStockConsumer
from ingestion.kafka_producer import StockTickerProducer
from load.lakehouse_loader import LakehouseLoader
from common.core.logger_config import setup_logger
from common.clients.api_client import _get_session
from schema.producer_schema import BaseMetadata
import sys
from common.core.exception import RetryableAPIError
import json
import time

logger = setup_logger(component="load")


def _load_main_message(
    config: BaseMetadata, mode: Literal["run_first", "run_retry"] = "run_first"
) -> None:
    """
    Executes a single pass of the load process for a given data type.

    It consumes messages from a Kafka topic, processes them via an external API,
    buffers the results, and flushes them to the Lakehouse in batches after cleansed by polars.
    Failed messages are forwarded to a designated retry or DLQ (Dead Letter Queue) topic.
    """
    BATCH_SIZE = config.batch_size
    buffer = []
    # STEP 1: Determine target and error topics based on the execution mode
    if mode == "run_first":
        target_table = config.data_type  # type: ignore
        error_topic = f"{config.data_type}_retry"  # type: ignore
        logger.info(
            f"Mode: FIRST RUN. Consuming from '{target_table}'. Forwarding errors to: '{error_topic}'."
        )
    elif mode == "run_retry":
        target_table = f"{config.data_type}_retry"  # type: ignore
        error_topic = f"{config.data_type}_dlq"  # type: ignore
        logger.info(
            f"Mode: RETRY. Consuming from '{target_table}'. Forwarding unrecoverable errors to DLQ: '{error_topic}'."
        )

    logger.info(f"Initiating data load to lakehouse for topic: '{config.data_type}'.")  # type: ignore

    loader = LakehouseLoader()
    # STEP 2: Initialize external connections (API session, Kafka consumer, Kafka producer)
    with (
        _get_session() as s,
        KafkaStockConsumer.managed(topic_name=target_table) as consumer,
        StockTickerProducer.managed(topic_name=error_topic) as producer,
    ):
        try:
            for raw_record in consumer.consume_message():
                ticker = raw_record.get("ticker", "unknown")

                try:  # STEP 3: Process the message and fetch actual data from the API
                    formatted_msg = consumer._process_single_message(
                        s, raw_record, transform_callable=config.transform_message
                    )  # type: ignore
                    if formatted_msg:
                        buffer.append(formatted_msg)

                except RetryableAPIError as e:
                    # STEP 4: Handle API failures by routing the message to the error topic
                    logger.warning(
                        f"Routing message for ticker '{ticker}' to '{error_topic}' due to error: {e}"
                    )
                    producer.single_message_data(
                        message=json.dumps(raw_record).encode("utf-8"),
                        key=raw_record.get("ticker", "unknown"),
                    )

                # STEP 5: Flush the buffer to the Lakehouse once the batch size is reached
                if len(buffer) >= BATCH_SIZE:
                    logger.info(
                        f"Batch capacity ({BATCH_SIZE}) reached. Initiating Transform & Load."
                    )
                    arrow_table = config._build_arrow_payload_lazy(buffer)  # type: ignore
                    if loader._put_lakehouse(
                        arrow_table=arrow_table, config=config, mode="append"
                    ):  # type: ignore
                        logger.info(
                            f"Successfully loaded batch of {BATCH_SIZE} records to Lakehouse. Committing offsets."
                        )
                        consumer._flush_and_commit(buffer)
        except Exception as e:
            logger.critical(f"critical failures: {e}", exc_info=True)
            raise e
        except KeyboardInterrupt:
            # Graceful shutdown on manual interrupt
            logger.warning(
                "Pipeline execution interrupted by user (KeyboardInterrupt). Halting load process."
            )
        finally:
            # STEP 6: Process any remaining messages in the buffer before shutting down
            if buffer:
                logger.info(
                    f"Processing {len(buffer)} remaining records in the buffer before shutdown."
                )
                try:
                    arrow_table = config._build_arrow_payload_lazy(buffer)  # type: ignore
                    loader._put_lakehouse(
                        arrow_table=arrow_table, config=config, mode="append"
                    )  # type: ignore
                    consumer._flush_and_commit(buffer)
                    logger.info(
                        "Successfully processed the remaining buffer during shutdown."
                    )
                except Exception as e:
                    logger.critical(
                        f"Critical failure while processing the remaining buffer: {e}",
                        exc_info=True,
                    )
                    raise e
            logger.info("Buffer is empty. Shutdown sequence complete.")


def load_main(model_cls: type[BaseMetadata]):
    """
    The main orchestrator for the data loading pipeline.

    This function manages a two-pass loading strategy:
    1. Primary run: Processes all fresh claim-check messages.
    2. Retry run: Attempts to process messages that failed during the primary run.
    """
    try:
        with model_cls() as config:  # type: ignore
            logger.info(f"Starting batch pipeline for data type: '{config.data_type}'.")

            # STEP 1: Execute the primary batch run
            _load_main_message(config=config, mode="run_first")
            time.sleep(10)
            # STEP 2: Execute the retry batch run for failed messages
            _load_main_message(config=config, mode="run_retry")
            logger.info(
                f"Batch pipeline completed successfully for '{config.data_type}'."
            )
    except Exception as e:
        logger.critical(
            f"Critical pipeline failure. Halting execution. Error: {e}", exc_info=True
        )
        sys.exit(1)
