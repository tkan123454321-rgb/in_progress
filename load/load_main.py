from typing import Any, Literal
from load.kafka_consumer import KafkaStockConsumer
from ingestion.kafka_producer import StockTickerProducer
from load.lakehouse_loader import LakehouseLoader
from utils.logger_config import setup_logger
from utils.other_utils import _get_session
import sys
from datetime import datetime
from zoneinfo import ZoneInfo
from polars.exceptions import PolarsError
from utils.exception import RetryableAPIError
import json

logger = setup_logger(component="load")


def _load_main_message(topic_name: str, mode: Literal["run_first", "run_retry"] = "run_first") -> None:
    BATCH_SIZE = 100
    buffer = []
    if mode == "run_first":
        target_table = topic_name                    # Luồng chính ghi vào bảng gốc
        error_topic = f"{topic_name}_retry"     # Lỗi thì vứt vào Topic Retry
        logger.info(f"🟢 [CHẾ ĐỘ: LẦN ĐẦU] Đọc từ {target_table} | Lỗi chuyển tiếp sang topic: '{error_topic}'")
    elif mode == "run_retry":
        target_table = f"{topic_name}_retry"    # Luồng retry vẫn ghi vào bảng gốc
        error_topic = f"{topic_name}_dlq"        # Cứu không được nữa thì vứt vào Nhà tù DLQ
        logger.info(f"🟢 [CHẾ ĐỘ: RETRY] Đọc từ {target_table} | Lỗi chuyển tiếp sang topic: '{error_topic}'")
    consumer = KafkaStockConsumer(topic_name=target_table)
    producer = StockTickerProducer(topic_name=error_topic)
    loader = LakehouseLoader()
    logger.info(f"🚀 Bắt đầu nạp dữ liệu vào lakehouse trên topic {topic_name}")
    
    
    with _get_session() as s:
        try:
            for raw_record in consumer.consume_message():
                
                try:
                    formatted_msg = consumer._process_single_message(s, raw_record)
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
                    logger.debug(f"Mẻ đã đầy {BATCH_SIZE} tin! Tiến hành Transform & Load...")
                    arrow_table = consumer._build_arrow_payload_lazy(buffer)
                    if loader._put_lakehouse(arrow_table, name_table=topic_name, mode ="append"):
                        consumer._flush_and_commit(buffer)
        except Exception as e:
            logger.critical(f"🔥 Lỗi nghiêm trọng toàn cục: {e}", exc_info=True)
            sys.exit(1)
        except KeyboardInterrupt:
            logger.warning("⏹️ Nhận lệnh dừng từ bàn phím (KeyboardInterrupt). Dừng quá trình nạp.")
        finally:            
            if buffer:
                logger.info(f"⚠️ Còn sót lại {len(buffer)} tin trong buffer chưa kịp nạp. Đang cố gắng xử lý nốt trước khi dừng...")
                try:
                    arrow_table = consumer._build_arrow_payload_lazy(buffer)
                    loader._put_lakehouse(arrow_table, name_table=topic_name, mode="append")
                    consumer._flush_and_commit(buffer)
                    logger.info(f"dừng thành công sau khi đã xử lý nốt buffer còn sót lại.")
                except Exception as e:
                    logger.critical(f"🔥 Lỗi nghiêm trọng khi xử lý buffer còn sót lại: {e}", exc_info=True)
            logger.info("✅ Không còn tin nào trong buffer. Dừng thành công!")
            consumer.consumer.close()


                    
        
def load_main(topic_name: str):
    logger.info("===========================================")
    logger.info(f"🚀 KHỞI ĐỘNG BATCH PIPELINE CHO: {topic_name}")
    logger.info("===========================================")
    
    # BƯỚC 1: Chạy mẻ chính
    _load_main_message(topic_name=topic_name, mode="run_first")
    
    # BƯỚC 2: Chạy mẻ vét đáy
    _load_main_message(topic_name=topic_name, mode="run_retry")
    
    logger.info("===========================================")
    logger.info(f"✅ HOÀN THÀNH TOÀN BỘ BATCH PIPELINE TỐT ĐẸP!")
    logger.info("===========================================")