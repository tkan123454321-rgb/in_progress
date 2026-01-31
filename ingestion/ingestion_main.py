from ingestion.ingestion_utils import CheckpointManager, get_ticker_list
from utils.logger_config import setup_logger
from ingestion.fetch_raw_financial_report import _fetch_financial_reports
from ingestion.kafka_adapter import KafkaAdminClient





logger = setup_logger(component="extract")
manager = CheckpointManager()
admin = KafkaAdminClient()


ticker_list = get_ticker_list()
todo_ticker_list = manager.filter_todo_list(ticker_list)

def balance_sheet():
    topic_name = "balance_sheet"
    admin.check_and_create_topic(topic_name)
    try:
        for ticker in todo_ticker_list:
            success = _fetch_financial_reports(ticker, topic_name = topic_name)
            if success:
                manager.mark_done(ticker)
    except KeyboardInterrupt:
        logger.warning("Nhận lệnh dừng từ bàn phím (KeyboardInterrupt). Dừng quá trình nạp báo cáo tài chính thành công.")
    except Exception as e:
        logger.critical(f"Lỗi nghiêm trọng: {e}")