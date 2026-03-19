
from typing import Any, Iterable, Callable, Literal
import uuid
from pydantic import ValidationError
from utils.logger_config import datetime, setup_logger
from ingestion.kafka_producer import StockTickerProducer
from utils.other_utils import ConfigLoader
from utils.postgres_client import PostgresClient
from zoneinfo import ZoneInfo
from schema.producer_schema import KafkaMetadataFundamental, BaseMetadata
from utils.kafka_client import KafkaClient
from utils.lakehouse_client import LakeHouseClient

logger = setup_logger(component="extract")

def ingest_fundamental_main[T: BaseMetadata](model_cls: type[T], generate_metadata_callable: Callable[[T, Iterable[str], str], Iterable[tuple[str, bytes]]], ticker_list_mode: Literal["fundamental", "other_data"] = "fundamental") -> None:
    config = ConfigLoader.load(model_cls)
    producer = StockTickerProducer(
        topic_name=config.topic
    )
    try:
        # Lấy danh sách tổng và ép kiểu Set
        ticker_list = LakeHouseClient()._get_ticker_list_raw(mode=ticker_list_mode)
        with PostgresClient.get_db_connection(db_name="ops_db") as conn:
            
            postgresclient = PostgresClient(conn)
            batch_id = str(uuid.uuid4())
            postgresclient.cleanup_state_table(table_name=config.table_name_postgres)
            missing_tickers = postgresclient.get_missing_tickers(table_name=config.table_name_postgres, tickers_set=ticker_list)
            
            if not missing_tickers:
                logger.info("Trống rỗng! Tất cả mã đã được nạp metadata cơ bản. Dừng thành công!")
                return
            
            logger.info(f"🚀 Bắt đầu tạo và bắn {len(missing_tickers)} mã vào Kafka. Batch ID: {batch_id}")
            metadata_generator = generate_metadata_callable(config, missing_tickers, batch_id)
            
            for ticker, metadata_item in metadata_generator:
                
                # Bắn vào Kafka (Không cần truyền topic_name nữa vì Producer đã nhớ)
                is_sent = producer.single_message_data(
                    message=metadata_item,
                    key=ticker
                )
                        
                if not is_sent:
                    raise RuntimeError(f"❌ Mất kết nối/Lỗi Kafka khi bắn mã {ticker}. Ngừng toàn bộ batch!")
                    # Ghi nhận trạng thái vào DB (Chỉ cần 2 tham số, mọi thứ khác class tự lo)
                postgresclient.log_metadata_to_db(ticker=ticker, batch_id=batch_id, topic=config.topic, data_type=config.data_type, table_name=config.table_name_postgres)                   
            logger.info(f"🏁 Đã đẩy xong batch {batch_id} vào Kafka. Chi tiết giám sát xem trên Grafana Dashboard.")
    except KeyboardInterrupt:
        logger.warning("⏹️ Nhận lệnh dừng từ bàn phím (KeyboardInterrupt). Dừng quá trình nạp.")
    except Exception as e:
        logger.critical(f"🔥 Lỗi nghiêm trọng toàn cục: {e}", exc_info=True)
    finally:
        if producer.close() is None:
            logger.info("🔒 Đã đóng producer sau khi nạp metadata cơ bản thành công.")
        else:
            logger.critical("⚠️ Chưa flush hết dữ liệu khi đóng producer sau nạp metadata cơ bản.")


    
    
