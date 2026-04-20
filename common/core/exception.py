from typing import Optional, Dict, Any

class PipelineBaseError(Exception):
    """Lớp gốc cho toàn bộ hệ thống Data Pipeline."""
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        self.message = message
        self.details = details or {}
        
        # Format hiển thị cực kỳ tường minh khi in ra log
        detail_str = f" | Details: {self.details}" if self.details else ""
        super().__init__(f"{self.message}: {detail_str}")


class RetryableAPIError(PipelineBaseError):
    """
    Bắn ra khi gặp sự cố mạng hoặc phía Server API (Timeout, 500, 502, 503).
    Báo hiệu cho luồng chính biết cần đem tin nhắn này đi thử lại.
    """
    def __init__(self, ticker: str, reason: Exception, message_id: Optional[str] = None):
        super().__init__(
            message=f"Sự cố API",
            details={
                "ticker": ticker, 
                "message_id": message_id,
                "reason_msg": f"{type(reason).__name__}: {str(reason)}"
            }
        )

class MetadataManagerError(PipelineBaseError):
    """
    Raised when metadata synchronization fails between storage layers,
    typically due to missing audit records or database inconsistency.
    """
    def __init__(self, table_name: str, message: str, details: Optional[Dict[str, Any]] = None):
        error_details = {"table_name": table_name}
        if details:
            error_details.update(details)
        super().__init__(message=f"Metadata Sync Failed: {message}", details=error_details)