from ingestion.ingestion_main import balance_sheet
from load.data_lake_client import DatalakeClient
from load.kafka_consumer import KafkaStockConsumer

# balance_sheet()
# datalake_client = DatalakeClient()
consumer = KafkaStockConsumer()

consumer.consume_batch_messages()