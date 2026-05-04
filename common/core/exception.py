from typing import Optional, Dict, Any


class PipelineBaseError(Exception):
    """
    Base exception class for the entire Data Pipeline system.

    All custom exceptions in the pipeline must inherit from this class.
    It provides a standardized structure to attach contextual metadata (details)
    to the error, making structured logging and debugging much easier.
    """

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        self.message = message
        self.details = details or {}

        # Format the output clearly for logs, avoiding trailing colons if details are empty
        error_msg = self.message
        if self.details:
            error_msg += f" | Details: {self.details}"

        super().__init__(error_msg)


class RetryableAPIError(PipelineBaseError):
    """
    Raised when a transient network or server-side API error occurs (e.g., Timeout, 500, 502, 503).

    This exception acts as a signal to the main orchestrator or Kafka consumer
    that the operation failed due to external factors, but it is safe to put
    the message back into the queue and retry later.
    """

    def __init__(
        self, ticker: str, reason: Exception, message_id: Optional[str] = None
    ):
        super().__init__(
            message="Transient API connectivity or server issue encountered",
            details={
                "ticker": ticker,
                "message_id": message_id,
                "error_type": type(reason).__name__,
                "error_reason": str(reason),
            },
        )


class MetadataManagerError(PipelineBaseError):
    """
    Raised when metadata synchronization fails between storage layers,
    typically due to missing audit records or database inconsistency.
    """

    def __init__(
        self, table_name: str, message: str, details: Optional[Dict[str, Any]] = None
    ):
        error_details = {"table_name": table_name}
        if details:
            error_details.update(details)
        super().__init__(
            message=f"Metadata Sync Failed: {message}", details=error_details
        )
