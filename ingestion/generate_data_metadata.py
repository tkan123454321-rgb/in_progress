from typing import Any, Iterable, Callable, Literal
import uuid
from schema.producer_schema import KafkaMetadataHistoricalQuotes
from utils import metadata_manager
from utils.logger_config import setup_logger
from utils.metadata_manager import MetadataManager
from utils.postgres_client import PostgresClient
from utils.lakehouse_client import LakeHouseClient



from ingestion.ingest_main import KafkaMetadataFundamental

logger = setup_logger(component="extract")

def _generate_metadata_fundamental(
    config: KafkaMetadataFundamental, 
    ticker_list: Iterable[str],
    metadata_manager: MetadataManager | None = None
) -> Iterable[tuple[str, bytes]]:
    """Chỉ gọi hàm nhân bản cực nhẹ của object."""
    for ticker in ticker_list:
        yield config._create_kafka_message(ticker=ticker)


def _generate_metadata_historical(config: KafkaMetadataHistoricalQuotes,  ticker_list: list[str], metadata_manager: MetadataManager
)-> Iterable[tuple[str, bytes]]:
    metadata_manager.sync_historical_watermark_tickers() 
    for ticker in ticker_list:
        start_date = metadata_manager._get_smart_start_date(ticker)
        # 1. Tính số ngày theo lịch (Calendar Days)
        calendar_days = (config.end_date - start_date).days 
        if calendar_days <= 0: 
            continue
        estimated_trading_days = int(calendar_days * (250 / 365)) + 20
        for current_offset in range(config.default_offset, estimated_trading_days, config.default_limit):
            yield config._create_kafka_message(
                ticker=ticker,
                start_date=start_date,
                offset = current_offset
            )


    