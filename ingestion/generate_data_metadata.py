from typing import Any, Iterable, Callable, Literal
import uuid
from schema.producer_schema import KafkaMetadataHistoricalQuotes
from utils.logger_config import setup_logger


from ingestion.ingest_main import KafkaMetadataFundamental

logger = setup_logger(component="extract")

def _generate_metadata_fundamental(
    config: KafkaMetadataFundamental, 
    ticker_list: Iterable[str]
) -> Iterable[tuple[str, bytes]]:
    """Chỉ gọi hàm nhân bản cực nhẹ của object."""
    for ticker in ticker_list:
        yield config._create_kafka_message(ticker=ticker)


def _generate_metadata_historical(self, config: KafkaMetadataHistoricalQuotes, tickers: list[str]):
        
        for ticker in tickers:
            start_date = get_smart_start_date(self.pg.conn, ticker)
            
            # 1. Tính số ngày theo lịch (Calendar Days)
            calendar_days = (config.end_date - start_date).days 
            if calendar_days <= 0: continue
            
            # 2. HACK TOÁN HỌC: Ước tính số ngày giao dịch thực tế
            # Cộng thêm một cái buffer khoảng 20 ngày để đề phòng sai số lễ tết
            estimated_trading_days = int(calendar_days * (250 / 365)) + 20
            
            # 3. Chạy vòng lặp theo số ngày ĐÃ ƯỚC TÍNH
            for current_offset in range(config.default_offset, estimated_trading_days, config.default_limit):
                yield config._create_kafka_message(
                    ticker=ticker,
                    start_date=start_date
                )


    