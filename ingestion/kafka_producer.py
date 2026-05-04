from typing import ClassVar
import socket
from typing import Any
from common.clients.kafka_client import KafkaClient
from confluent_kafka import Producer
from confluent_kafka.error import ProduceError
from confluent_kafka.cimpl import KafkaException
import os
import time
from common.core.logger_config import setup_logger
from contextlib import contextmanager

logger = setup_logger(component="ingest")


class StockTickerProducer(KafkaClient):
    """
    A Kafka Producer specifically designed for sending stock ticker data.

    This class handles message production with built-in retry mechanisms,
    buffer management, and graceful shutdowns.
    """

    _PRODUCER_CONFIG: ClassVar[dict[str, Any]] = {
        "enable.idempotence": True,  # Ensures strictly exactly-once semantics (prevents duplicates)
        "compression.type": "lz4",  # Compresses batch data
        "linger.ms": 50,  # Waits 50ms to group messages into a batch before dispatching
    }

    def __init__(
        self,
        topic_name: str,
        update_conf: dict[str, str | int | float | bool] | None = None,
    ):
        """
        Initializes the StockTickerProducer.

        Args:
            topic_name (str): The target Kafka topic to produce messages to.
            update_conf (dict, optional): Additional Kafka producer configurations
                                          to override or append to the defaults.
        """
        super().__init__(topic_name)

        pid = os.getpid()
        hostname = socket.gethostname()
        self.topic_name = topic_name
        self.client_id = f"stock-producer-{hostname}-{pid}"
        self.conf = {
            "bootstrap.servers": self.DEFAULT_BOOTSTRAP,
            "client.id": self.client_id,
            **self._PRODUCER_CONFIG,
        }

        if update_conf:
            self.conf.update(update_conf)

        logger.info(
            f"Initializing Kafka Producer. Topic: '{self.topic_name}', Client ID: '{self.client_id}'."
        )
        self.producer = Producer(self.conf)

    def _on_delivery_report(self, err, msg) -> None:
        """
        Callback triggered by Kafka once a message is successfully delivered or fails.
        """
        if err is not None:
            logger.error(
                f"Message delivery failed. Topic: '{msg.topic()}', Error: {err}",
                exc_info=True,
            )
        else:
            logger.debug(
                f"Message delivered successfully. "
                f"Topic: '{msg.topic()}', Partition: {msg.partition()}, Offset: {msg.offset()}."
            )

    def single_message_data(self, message, key) -> bool:
        """
        Produces a single message to Kafka with a retry mechanism.

        Args:
            message (bytes): The payload of the message to send.
            key (str): The partition key (Stock Ticker symbol).

        Returns:
            bool: True if successfully queued, False if it fails after max retries.
        """
        retry_delay = 0.2
        attempt = 0
        while True:
            try:
                self.producer.produce(
                    topic=self.topic_name,
                    value=message,
                    on_delivery=self._on_delivery_report,
                    key=key,
                )
                self.producer.poll(0)
                logger.debug(
                    f"Queued single message. Topic: '{self.topic_name}', Key: '{key}'."
                )
                return True
            except BufferError as e:
                logger.warning(
                    f"Local buffer full. Retrying in {retry_delay}s. Key: '{key}', Error: {e}"
                )
                self.producer.poll(retry_delay)
            except ProduceError as e:
                kafka_error = e.args[0]
                if kafka_error.retriable():
                    attempt += 1
                    if attempt > 5:
                        logger.error(
                            f"Max retries (5) exceeded for single message. Key: '{key}', Error: {kafka_error}",
                            exc_info=True,
                        )
                        return False

                    logger.warning(
                        f"Retriable Kafka error. Attempt: {attempt}/5. Retrying in {retry_delay}s. Key: '{key}'."
                    )
                    time.sleep(retry_delay)
                    self.producer.poll(0)
                else:
                    logger.error(
                        f"Non-retriable Kafka error encountered. Key: '{key}', Error: {kafka_error}",
                        exc_info=True,
                    )
                    return False

    def batch_message_data(self, messages_list: list[bytes], key: str) -> bool:
        """
        Produces a batch of messages to Kafka sharing the same partition key.

        Args:
            messages_list (list[bytes]): List of payloads for the same ticker (e.g., Financial statements).
            key (str): The partition key (Stock Ticker symbol).

        Returns:
            bool: True if the entire batch is queued successfully, False otherwise.
        """
        formatted_messages = [{"value": msg, "key": key} for msg in messages_list]

        retry_delay = 0.2
        attempt = 0

        while True:
            try:
                num_queued = self.producer.produce_batch(
                    topic=self.topic_name,
                    messages=formatted_messages,
                    on_delivery=self._on_delivery_report,
                )
                self.producer.poll(0)

                logger.debug(
                    f"Batch queued successfully. "
                    f"Topic: '{self.topic_name}', Key: '{key}', Messages: {num_queued}/{len(messages_list)}."
                )
                return True

            except BufferError as e:
                logger.warning(
                    f"Batch buffer full. Retrying in {retry_delay}s. Key: '{key}', Error: {e}"
                )
                self.producer.poll(retry_delay)

            except KafkaException as e:
                kafka_error = e.args[0]

                if kafka_error.retriable():
                    attempt += 1
                    if attempt > 5:
                        logger.error(
                            f"Max retries (5) exceeded for batch message. Key: '{key}', Error: {kafka_error}",
                            exc_info=True,
                        )
                        return False

                    logger.warning(
                        f"Retriable Kafka error in batch. Attempt: {attempt}/5. Retrying in {retry_delay}s. Key: '{key}'."
                    )
                    time.sleep(retry_delay)
                    self.producer.poll(0)
                else:
                    logger.error(
                        f"Non-retriable Kafka error in batch. Key: '{key}', Error: {kafka_error}",
                        exc_info=True,
                    )
                    return False

    def close(self) -> int:
        """
        Flushes outstanding messages to Kafka before shutting down the producer.

        Returns:
            int: The number of messages still in the queue after flushing.
                 Returns 0 if all messages were successfully delivered.
        """
        logger.info(
            f"Flushing producer queue for topic '{self.topic_name}' before shutting down."
        )
        return self.producer.flush(10)

    @classmethod
    @contextmanager
    def managed(cls, topic_name: str, update_conf: dict | None = None):
        """
        Factory method providing a Context Manager for the Producer.
        Ensures graceful shutdown and flushing of messages upon exit.

        Usage:
            with StockTickerProducer.managed("my_topic") as producer:
                producer.single_message_data(msg, key)
        """
        logger.info(
            f"Opening managed Kafka producer connection for topic '{topic_name}'."
        )
        producer_instance = cls(topic_name=topic_name, update_conf=update_conf)

        try:
            yield producer_instance

        finally:
            remaining_messages = producer_instance.close()
            if remaining_messages == 0:
                logger.info(
                    f"Producer closed gracefully. All messages flushed for topic '{topic_name}'."
                )
            else:
                logger.critical(
                    f"Producer closed with unflushed messages. Topic: '{topic_name}', Dropped count: {remaining_messages}."
                )
