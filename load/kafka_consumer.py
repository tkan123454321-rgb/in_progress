import time
import requests
from common.core.exception import RetryableAPIError
from common.core.logger_config import setup_logger
from confluent_kafka import (
    Consumer,
    KafkaError,
    KafkaException,
    Message,
    TopicPartition,
)
import os
import socket
import json
from common.clients.lakehouse_client import LakeHouseClient
from common.clients.kafka_client import KafkaClient
from typing import Any, Callable, Iterable, ClassVar, Sequence
from contextlib import contextmanager

logger = setup_logger(component="load")


class KafkaStockConsumer(KafkaClient):
    """
    Consumes claim-check metadata from Kafka, fetches heavy payloads via API,
    and manages manual offset commits for exactly-once processing.
    """

    # Kafka Consumer Configurations
    # Disable auto-commit to ensure we only commit AFTER data is safely saved to the Lakehouse
    CONSUMER_CONFIG: ClassVar[dict[str, Any]] = {
        "max.poll.interval.ms": 350000,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    }
    # Polling limits and timeout thresholds
    MESSAGE_SIZE: ClassVar[int] = 10
    POLL_TIMEOUT: ClassVar[float] = 3.0
    MAX_EMPTY_POLLS: ClassVar[int] = 3

    def __init__(
        self,
        topic_name="default",
        update_conf: dict[str, str | int | float | bool] | None = None,
    ):
        """
        Initializes the Kafka Consumer with specific group and client IDs.
        """

        self.topic_name = topic_name
        self._loader = LakeHouseClient()
        self.group_id = f"consumer-{topic_name}-group"
        pid = os.getpid()
        hostname = socket.gethostname()
        self.client_id = f"stock-consumer-{hostname}-{pid}"

        self.conf = {
            "bootstrap.servers": self.DEFAULT_BOOTSTRAP,
            "client.id": self.client_id,
            "group.id": self.group_id,
            **self.CONSUMER_CONFIG,
        }
        if update_conf:
            self.conf.update(update_conf)
            logger.debug(f"Consumer configuration overridden: {self.conf}")

        self.consumer = Consumer(self.conf)
        logger.info(
            f"Kafka Consumer successfully initialized. "
            f"Topic: '{self.topic_name}', Group ID: '{self.group_id}', Client ID: '{self.client_id}'."
        )

    def consume_message(self) -> Iterable[dict[str, Any]]:
        """
        Continuously polls Kafka for new messages. Yields successfully JSON payloads.
        Gracefully exits if no new messages are found after a defined number of empty polls.
        """
        EMPTY_POLL = 0
        self.consumer.subscribe(topics=[self.topic_name])
        logger.info(f"Subscribed to topic '{self.topic_name}'. Polling for messages...")

        while True:
            try:
                # Fetch a batch of messages
                messages: Sequence[Message | None] = self.consumer.consume(
                    num_messages=self.MESSAGE_SIZE, timeout=self.POLL_TIMEOUT
                )
                # Handle timeout/empty batches
                if not messages:
                    EMPTY_POLL += 1
                    logger.info(
                        f"Empty poll ({EMPTY_POLL}/{self.MAX_EMPTY_POLLS}) for topic '{self.topic_name}'."
                    )
                    if EMPTY_POLL >= self.MAX_EMPTY_POLLS:
                        logger.info(
                            "Max empty polls reached. Assuming topic is fully consumed. Halting poll loop."
                        )
                        break
                    continue
                # Reset counter since we received valid messages
                EMPTY_POLL = 0
                for msg in messages:
                    msg_error = msg.error()
                    if msg_error:
                        # PARTITION_EOF is a standard notification, not an actual error
                        if msg_error.code() == KafkaError._PARTITION_EOF:  # type: ignore
                            logger.debug(
                                f"Reached end of partition: {msg.topic()} [{msg.partition()}]"
                            )
                        else:
                            logger.error(
                                f"Message extraction error. Code: {msg_error.code()}, Details: {msg_error.str()}"
                            )
                        continue

                    if msg.value() is None:
                        continue

                    try:
                        yield json.loads(msg.value().decode("utf-8"))  # type: ignore
                    except (json.JSONDecodeError, UnicodeDecodeError) as e:
                        logger.error(
                            f"Failed to decode Kafka message payload. Error: {e}"
                        )
                        continue

            except KafkaException as e:
                logger.critical(
                    f"Critical Kafka cluster exception during polling: {e}",
                    exc_info=True,
                )
                raise e

    def _flush_and_commit(self, buffer: list[dict[str, Any]]) -> None:
        """
        Commits the current Kafka offsets synchronously and clears the local processing buffer.
        """
        if not buffer:
            return

        try:
            # Sync commit ensures we don't proceed until the broker acknowledges the offset
            topic_partitions: list[TopicPartition] = self.consumer.commit(
                asynchronous=False
            )
            logger.info(
                f"Kafka offsets committed and populated lakehouse successfully. Partitions: {topic_partitions}"
            )

        except KafkaException as e:
            err = e.args[0]
            # Handle edge case where there are no new offsets to commit
            if err.code() == KafkaError._NO_OFFSET:  # type: ignore
                logger.warning("No new offsets to commit. Skipping safely.")
            else:
                logger.error(
                    f"Critical failure during offset commit: {err}", exc_info=True
                )
                raise e
        finally:
            # Always clear the memory buffer, even if the commit fails
            buffer.clear()
            logger.info("Local processing buffer cleared.")

    @staticmethod
    def _process_single_message(
        request_session: requests.Session,
        msg: dict[str, Any],
        transform_callable: Callable[[dict[str, Any], Any], list],
    ) -> list | None:
        """
        Executes the heavy data extraction via API using the metadata from the Kafka claim-check ticket.
        """
        ticker = msg.get("ticker", "unknown")
        url = msg.get("url")
        if not url:
            logger.warning(
                f" message skipped. Missing 'url' field. Ticker: '{ticker}', Message: {msg}"
            )
            return None
        try:
            # Basic rate-limiting to avoid overwhelming the target API
            time.sleep(0.1)
            response = request_session.get(url, timeout=10)

            response.raise_for_status()

            api_data = response.json()

            if not api_data:
                logger.info(
                    f"No data returned from API for ticker '{ticker}'. URL: {url}. Skipping."
                )
                return None
            else:
                logger.debug(
                    f"Successfully fetched API payload for ticker '{ticker}'. Initiating transformation."
                )
            return transform_callable(msg, api_data)
        except (requests.RequestException, json.JSONDecodeError) as e:
            raise RetryableAPIError(
                ticker=ticker, reason=e, message_id=msg.get("message_id")
            ) from e

    @classmethod
    @contextmanager
    def managed(cls, topic_name: str, update_conf: dict[str, Any] | None = None):
        """
        Context Manager ensuring graceful initialization and shutdown of the Kafka Consumer.
        """
        logger.info(
            f"Establishing managed Kafka Consumer context for topic '{topic_name}'..."
        )
        consumer_instance = cls(topic_name=topic_name, update_conf=update_conf)

        try:
            yield consumer_instance
        finally:
            logger.info(
                f"Tearing down Kafka Consumer context for topic '{topic_name}'..."
            )
            try:
                consumer_instance.consumer.close()
                logger.info("Kafka Consumer closed gracefully.")
            except Exception as e:
                logger.error(
                    f"Error encountered while closing Kafka Consumer: {e}",
                    exc_info=True,
                )
