from typing import Any, Iterable, Callable, Literal
import uuid
from common.core.logger_config import setup_logger
from ingestion.kafka_producer import StockTickerProducer
from common.clients.postgres_client import PostgresClient
from schema.producer_schema import BaseMetadata
from common.clients.lakehouse_client import LakeHouseClient
from common.infrastructure.metadata_manager import MetadataManager

logger = setup_logger(component="ingest")
type GeneratorFunc[T] = Callable[[T, Iterable[str]], Iterable[tuple[str, bytes]]]

def ingest_main[T: BaseMetadata](model_cls: type[T]) -> None:
    """
    The main orchestrator for the daily data ingestion pipeline.
    
    This function coordinates these steps: fetching active tickers, 
    determining which ones need processing, generating claim-check messages, 
    publishing them to Kafka, and recording the state in Postgres.
    """
    BATCH_ID = str(uuid.uuid4())
    logger.info(f"Starting ingestion pipeline. Assigned Batch ID: '{BATCH_ID}'.")
    
    # Instantiate the specific configuratio data model
    config = model_cls() # type: ignore
    metadata_manager = MetadataManager(
        pg_client=PostgresClient(),
        lake_client=LakeHouseClient())
    try:
        # STEP 2: Establish Contexts (Database & Kafka Connections)
        # Using 'with' context managers ensures connections are safely closed even if crashes occur.
        with metadata_manager as metadata_manager, StockTickerProducer.managed(topic_name=config.data_type) as producer: 
        # STEP 3: State Reconciliation
            # 3.1. Fetch the reference list of active tickers from the Lakehouse.
            ticker_list = metadata_manager._get_ticker_list_raw(mode=config.ticker_list_mode)  # type: ignore
            # 3.2. Clean up expired claim-check states (older than 24h) from previous runs
            metadata_manager.cleanup_kafka_state(table_name=config.table_name_postgres, data_type=config.data_type) # type: ignore
            # 3.3. Compare reference list vs today's successful records to find missing ones.
            missing_tickers = metadata_manager.get_missing_tickers(table_name=config.table_name_postgres, 
                                                                   tickers_set=ticker_list, 
                                                                   data_type=config.data_type # type: ignore
                                                                   ) 
            # STEP 4: Exit early if there's no work to do
            if not missing_tickers:
                logger.info(f"Pipeline completed early. All tickers for '{config.data_type}' are already processed today.")
                return
            
            logger.info(f"Initiating message generation for {len(missing_tickers)} missing tickers. Target Topic: '{config.data_type}'.")
            # STEP 5: Generate and Publish Messages
            # We iterate through the generator which yields payloads for each ticker.
            for ticker, metadata_items in config._generate_kafka_message(ticker_list=missing_tickers, metadata_manager=metadata_manager, batch_id=BATCH_ID):  # type: ignore
                
                # Publish the payload batch to the Kafka broker
                is_sent = producer.batch_message_data(
                    messages_list=metadata_items,
                    key=ticker
                )
                # STEP 6: Fail-fast and State Logging
                if not is_sent:
                    error_msg = f"Kafka production failed for ticker '{ticker}'. Halting the entire batch."
                    logger.critical(error_msg)
                    raise RuntimeError(error_msg)
                # If successfully published, log the claim-check ticket into Postgres
                metadata_manager.log_metadata_to_db(ticker=ticker, batch_id=BATCH_ID, data_type=config.data_type, table_name=config.table_name_postgres)                    # type: ignore
            logger.info(f"Ingestion batch '{BATCH_ID}' completed successfully. Monitoring available on Grafana.")
    except KeyboardInterrupt:
        # Graceful shutdown on manual interrupt (Ctrl+C during local dev)
        logger.warning("Pipeline execution interrupted by user (KeyboardInterrupt). Shutting down gracefully.")
    except Exception as e:
        logger.critical(f"Critical failure in ingestion pipeline. Error: {e}", exc_info=True)




    
    
