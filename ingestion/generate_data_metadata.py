from typing import Any, Iterable, Callable, Literal
import uuid
from utils.logger_config import setup_logger


from ingestion.ingest_main import KafkaMetadataFundamental

logger = setup_logger(component="extract")

def _generate_metadata_fundamental[T: KafkaMetadataFundamental](
    config: KafkaMetadataFundamental, 
    ticker_list: Iterable[str],
    batch_id: str
) -> Iterable[tuple[str, bytes]]:
    """Chỉ gọi hàm nhân bản cực nhẹ của object."""
    for ticker in ticker_list:
        yield config._create_kafka_message(ticker=ticker, batch_id=batch_id)



    