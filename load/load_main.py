from typing import Any, Literal
from load.kafka_consumer import KafkaStockConsumer
from ingestion.kafka_producer import StockTickerProducer
from load.lakehouse_loader import LakehouseLoader
from utils.logger_config import setup_logger
from utils.other_utils import _get_session
from schema.producer_schema import BaseMetadata
import sys
from utils.exception import RetryableAPIError
import json
import time

logger = setup_logger(component="load")


def _load_main_message(config: BaseMetadata, mode: Literal["run_first", "run_retry"] = "run_first") -> None:
    BATCH_SIZE = config.batch_size
    buffer = []
    if mode == "run_first":
        target_table = config.data_type                 # type: ignore # Luồng chính ghi vào bảng gốc
        error_topic = f"{config.data_type}_retry"     # type: ignore # Lỗi thì vứt vào Topic Retry
        logger.info(f"🟢 [CHẾ ĐỘ: LẦN ĐẦU] Đọc từ {target_table} | Lỗi chuyển tiếp sang topic: '{error_topic}'")
    elif mode == "run_retry":
        target_table = f"{config.data_type}_retry"    # type: ignore # Luồng retry vẫn ghi vào bảng gốc
        error_topic = f"{config.data_type}_dlq"        # type: ignore # Cứu không được nữa thì vứt vào Nhà tù DLQ
        logger.info(f"🟢 [CHẾ ĐỘ: RETRY] Đọc từ {target_table} | Lỗi chuyển tiếp sang topic: '{error_topic}'")
        
    logger.info(f"🚀 Bắt đầu nạp dữ liệu vào lakehouse trên topic {config.data_type}") # type: ignore
    
    loader = LakehouseLoader()
    with _get_session() as s, \
        KafkaStockConsumer.managed(topic_name=target_table) as consumer, \
        StockTickerProducer.managed(topic_name=error_topic) as producer:
        try:
            for raw_record in consumer.consume_message():
                ticker = raw_record.get("ticker", "unknown")
                
                try:
                    formatted_msg = consumer._process_single_message(s, raw_record, transform_callable = config.transform_message) # type: ignore
                    # 3. Nếu API trả về ngon lành thì nhét vào giỏ
                    if formatted_msg:
                        buffer.append(formatted_msg)
                        
                        
                        
                except RetryableAPIError as e:
                    logger.warning(f"🔄 Đẩy tin nhắn vào {error_topic} do lỗi: {e}")
                    producer.single_message_data(
                        message=json.dumps(raw_record).encode('utf-8'),
                        key=raw_record.get("ticker", "unknown")
                    )

                # 4. GOM ĐỦ MẺ THÌ XẢ KHO (FLUSH)
                if len(buffer) >= BATCH_SIZE:
                    logger.info(f"Mẻ đã đầy {BATCH_SIZE} tin! Tiến hành Transform & Load...")
                    arrow_table = config._build_arrow_payload_lazy(buffer) # type: ignore
                    if loader._put_lakehouse(arrow_table=arrow_table, config=config, mode ="append"): # type: ignore
                        logger.info(f"✅ Đã nạp thành công mẻ {BATCH_SIZE} tin vào Lakehouse. Tiến hành commit Kafka...")
                        consumer._flush_and_commit(buffer)
        except Exception as e:
            logger.critical(f"🔥 Lỗi nghiêm trọng toàn cục: {e}", exc_info=True)
            raise e
        except KeyboardInterrupt:
            logger.warning("⏹️ Nhận lệnh dừng từ bàn phím (KeyboardInterrupt). Dừng quá trình nạp.")
        finally:            
            if buffer:
                logger.info(f"⚠️ Còn sót lại {len(buffer)} tin trong buffer chưa kịp nạp. Đang cố gắng xử lý nốt trước khi dừng...")
                try:
                    arrow_table = config._build_arrow_payload_lazy(buffer) # type: ignore
                    loader._put_lakehouse(arrow_table=arrow_table, config=config, mode="append") # type: ignore
                    consumer._flush_and_commit(buffer)
                    logger.info(f"dừng thành công sau khi đã xử lý nốt buffer còn sót lại.")
                except Exception as e:
                    logger.critical(f"🔥 Lỗi nghiêm trọng khi xử lý buffer còn sót lại: {e}", exc_info=True)
                    raise e
            logger.info("✅ Không còn tin nào trong buffer. Dừng thành công!")



                    
        
def load_main(model_cls: type[BaseMetadata]):
    try:
        with model_cls() as config: # type: ignore
            logger.info(f"🚀 KHỞI ĐỘNG BATCH PIPELINE CHO: {config.data_type}") 
            
            # BƯỚC 1: Chạy mẻ chính
            _load_main_message(config = config, mode="run_first")
            time.sleep(10)
            # BƯỚC 2: Chạy mẻ vét đáy
            _load_main_message(config = config, mode="run_retry")
            logger.info(f"✅ HOÀN THÀNH TOÀN BỘ BATCH PIPELINE TỐT ĐẸP!")
    except Exception as e:
        logger.critical(f"🔥 Lỗi nghiêm trọng toàn cục: {e}", exc_info=True)
        sys.exit(1)