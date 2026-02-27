import json
import uuid
from utils.lakehouse_read import LakehouseReader
from utils.logger_config import datetime, setup_logger
from ingestion.kafka_producer import StockTickerProducer
from utils.lakehouse_connection import check_and_create_topic
from utils.postgres_client import cleanup_state_table, get_db_connection, get_missing_tickers, log_metadata_to_db, CONN_STR


logger = setup_logger(component="extract")
bootstrap_servers = 'kafka:29092'

def _generate_basic_metadata(ticker_list, batch_id=None):
    if not batch_id:
        batch_id = str(uuid.uuid4())
    logger.info(f"Batch_id: {batch_id} | Số mã cần nạp: {len(ticker_list)}")
    total_generated = 0
    
    for ticker in ticker_list:
        task_meta = {
            "batch_id": batch_id,      
            "ticker": ticker,
            "data_type": "fundamental", 
            "year": None,              
            "quarter": None,           
            "source": "fireant",
            "created_at_ts": datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
            "url": f"https://restv2.fireant.vn/symbols/{ticker}/fundamental"
        }    
        total_generated += 1
        logger.debug(f"Generated metadata for {ticker} | Total generated: {total_generated}")
        yield task_meta
    

def _ingest_fundamental(metadata_generator, producer, topic, conn, table_name_postgres, batch_id):
    for metadata_item in metadata_generator:
        ticker = metadata_item["ticker"]
        message_bytes = json.dumps(metadata_item, ensure_ascii=False).encode('utf-8')
        
        # Bắn vào Kafka
        is_sent = producer.single_message_data(
                message=message_bytes,
                key=ticker,
                topic_name=topic)
                
        if is_sent:
            logger.debug(f"✅ Nạp metadata cơ bản thành công cho {ticker}")
            # Ghi nhận trạng thái vào DB để Grafana query lên chart
            log_metadata_to_db(conn, table_name=table_name_postgres, batch_id=batch_id, topic=topic, ticker=ticker, data_type="fundamental")
        else:
            # Bắn lỗi ra log, bỏ qua để mai chạy bù
            logger.error(f"❌ Nạp metadata cơ bản thất bại cho {ticker}. Bỏ qua nạp lại sau.")




def ingest_fundamental_main():
    topic = "fundamental"
    table_name_postgres = "ingestion_metadata_fundamental"
    client_id = 'producer-fundamental'
    check_topic = check_and_create_topic(topic_name=topic, bootstrap_servers=bootstrap_servers)
    if not check_topic:
        logger.critical(f"🚨 Không thể tạo topic '{topic}'. Dừng quá trình nạp dữ liệu cơ bản.")
        return
        
    producer = StockTickerProducer(
        bootstrap_servers=bootstrap_servers, # type: ignore
        client_id=client_id,
        topic_name=topic
    )
    try:
        # Lấy danh sách tổng và ép kiểu Set
        ticker_list = set(LakehouseReader()._get_ticker_list_raw())
        
        with get_db_connection() as conn:
            batch_id = str(uuid.uuid4())
            cleanup_state_table(conn, table_name_postgres)
            missing_tickers = get_missing_tickers(CONN_STR, table_name_postgres, ticker_list)
            if not missing_tickers:
                logger.info("Trống rỗng! Tất cả mã đã được nạp metadata cơ bản. Dừng thành công!")
                return
            logger.info(f"🚀 Bắt đầu tạo và bắn {len(missing_tickers)} mã vào Kafka. Batch ID: {batch_id}")
            metadata_generator = _generate_basic_metadata(ticker_list=missing_tickers, batch_id=batch_id)
            _ingest_fundamental(
                metadata_generator=metadata_generator,
                producer=producer,
                topic=topic,
                conn=conn,
                table_name_postgres=table_name_postgres,
                batch_id=batch_id
            )
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

    
    
