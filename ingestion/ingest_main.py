from typing import Any, Iterable, Callable, Literal
import uuid
from pydantic import ValidationError
from utils.logger_config import datetime, setup_logger
from ingestion.kafka_producer import StockTickerProducer
from utils.postgres_client import PostgresClient
from zoneinfo import ZoneInfo
from schema.producer_schema import KafkaMetadataFundamental, BaseMetadata
from utils.kafka_client import KafkaClient
from utils.lakehouse_client import LakeHouseClient
from utils.metadata_manager import MetadataManager

logger = setup_logger(component="extract")
type GeneratorFunc[T] = Callable[[T, Iterable[str]], Iterable[tuple[str, bytes]]]

def ingest_main[T: BaseMetadata](model_cls: type[T], generate_metadata_callable: GeneratorFunc[T], ticker_list_mode: Literal["fundamental", "other_data"] = "fundamental") -> None:
    config = model_cls() # type: ignore
    metadata_manager = MetadataManager(
        pg_client=PostgresClient(),
        lake_client=LakeHouseClient())
    try:
        with metadata_manager as metadata_manager, StockTickerProducer.managed(topic_name=config.topic) as producer: # type: ignore
        # Lấy danh sách tổng và ép kiểu Set
            ticker_list = metadata_manager._get_ticker_list_raw(mode=ticker_list_mode)
            metadata_manager.cleanup_state_table(table_name=config.table_name_postgres) # type: ignore
            missing_tickers = metadata_manager.get_missing_tickers(table_name=config.table_name_postgres, tickers_set=ticker_list, data_type=config.data_type) # type: ignore
            
            if not missing_tickers:
                logger.info("Trống rỗng! Tất cả mã đã được nạp metadata cơ bản. Dừng thành công!")
                return
            
            logger.info(f"🚀 Bắt đầu tạo và bắn {len(missing_tickers)} mã vào Kafka. Batch ID: {config.batch_id}")
            metadata_generator = generate_metadata_callable(config=config, ticker_list=missing_tickers, metadata_manager=metadata_manager) # type: ignore
            for ticker, metadata_items in metadata_generator:
                
                # Bắn vào Kafka (Không cần truyền topic_name nữa vì Producer đã nhớ)
                is_sent = producer.batch_message_data(
                    messages_list=metadata_items,
                    key=ticker
                )
                        
                if not is_sent:
                    raise RuntimeError(f"❌ Mất kết nối/Lỗi Kafka khi bắn mã {ticker}. Ngừng toàn bộ batch!")
                    # Ghi nhận trạng thái vào DB (Chỉ cần 2 tham số, mọi thứ khác class tự lo)
                metadata_manager.log_metadata_to_db(ticker=ticker, batch_id=config.batch_id, topic=config.topic, data_type=config.data_type, table_name=config.table_name_postgres)                    # type: ignore
            logger.info(f"🏁 Đã đẩy xong batch {config.batch_id} vào Kafka. Chi tiết giám sát xem trên Grafana Dashboard.")
    except KeyboardInterrupt:
        logger.warning("⏹️ Nhận lệnh dừng từ bàn phím (KeyboardInterrupt). Dừng quá trình nạp.")
    except Exception as e:
        logger.critical(f"🔥 Lỗi nghiêm trọng toàn cục: {e}", exc_info=True)



    
    
