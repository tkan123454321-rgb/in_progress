import math
from typing import Any, Iterable, Callable, Literal, List, Tuple, Generator
import uuid
from schema.producer_schema import HistoricalQuotes, Fundamental, FinancialReports
from utils import metadata_manager
from utils.logger_config import setup_logger
from utils.metadata_manager import MetadataManager
from utils.postgres_client import PostgresClient
from utils.lakehouse_client import LakeHouseClient



logger = setup_logger(component="extract")

def _generate_metadata_fundamental(batch_id: str,
    config: Fundamental, 
    ticker_list: Iterable[str],
    metadata_manager: MetadataManager | None = None,
) -> Iterable[Tuple[str, List[bytes]]]:
    """Chỉ gọi hàm nhân bản cực nhẹ của object."""
    for ticker in ticker_list:
        ticker, message_bytes = config._create_kafka_message(ticker=ticker, batch_id=batch_id)
        
        # Bọc cái message_bytes vào trong 1 cái list
        
        yield ticker, [message_bytes]


def _generate_metadata_historical(config: HistoricalQuotes,  ticker_list: list[str], metadata_manager: MetadataManager, batch_id: str
)-> Iterable[Tuple[str, List[bytes]]]:
    metadata_manager.sync_historical_watermark_tickers(table_name_watermark_postgres=config.table_watermark_name_postgres) # type: ignore
    for ticker in ticker_list:
        start_date = metadata_manager._get_smart_start_date(ticker, table_name=config.table_watermark_name_postgres) # type: ignore
        # 1. Tính số ngày theo lịch (Calendar Days)
        calendar_days = (config.end_date - start_date).days 
        if calendar_days <= 0: 
            continue
        estimated_trading_days = int(calendar_days * (250 / 365)) + 20
        batch_messages = []
        for current_offset in range(config.default_offset, estimated_trading_days, config.default_limit):
            ticker, message_bytes = config._create_kafka_message(
                ticker=ticker,
                start_date=start_date,
                offset=current_offset,
                batch_id = batch_id
            )
            # Nhét vào giỏ
            batch_messages.append(message_bytes)
            yield ticker, batch_messages

def _generate_quarter_metadata_financial_reports(config: FinancialReports,  ticker_list: list[str], metadata_manager: MetadataManager, batch_id: str
)-> Iterable[Tuple[str, List[bytes]]]:
    metadata_manager.sync_historical_watermark_tickers(table_name_watermark_postgres=config.table_watermark_name_postgres) # type: ignore
    for ticker in ticker_list:
        start_date = metadata_manager._get_smart_start_date(ticker, table_name_watermark_postgres=config.table_watermark_name_postgres) # type: ignore
        start_year = start_date.year
        start_quarter = math.ceil(start_date.month / 3)
        limit = (config.target_year - start_year) * 4 + (config.target_quarter - start_quarter) 
        limit += 2
        if limit <= 0:
            logger.warning(
                f"[FINANCIAL_REPORTS][SKIP_TICKER] Bỏ qua mã {ticker:<5} | "
                f"Lý do: Watermark DB (Q{start_quarter}/{start_year}: {start_date}) >= Target (Q{config.target_quarter}/{config.target_year}) | "
                f"Calc_Limit: {limit} <= 0"
            )
            continue

        ticker, message_bytes = config._create_kafka_message(
            ticker=ticker,
            limit=limit,
            type_id=config.type_id,
            batch_id=batch_id
        )
        # Nhét vào giỏ
        yield ticker, [message_bytes]
      
        
    