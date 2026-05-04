import requests
import os
from common.core.logger_config import setup_logger

logger = setup_logger(component="load")


def _get_session() -> requests.Session:
    """
    Initializes and configures a reusable HTTP requests session.

    Using a Session object improves pipeline performance by enabling connection pooling
    (reusing the underlying TCP connection) for consecutive API requests to the same host.
    It also securely attaches necessary authentication and routing headers fetched
    from the environment variables.

    Returns:
        requests.Session: A fully configured session object ready for executing API calls.

    Raises:
        EnvironmentError: If critical environment variables are missing.
    """
    s = requests.Session()
    auth_token = os.getenv("AUTH_TOKEN")
    user_agent = os.getenv("USER_AGENT")
    source = os.getenv("MY_SOURCE")
    if not auth_token or not user_agent or not source:
        error_msg = "Missing required environment variables (AUTH_TOKEN, USER_AGENT, or MY_SOURCE)."
        logger.critical(error_msg)
        raise EnvironmentError(error_msg)

    s.headers.update(
        {
            "User-Agent": user_agent,
            "Accept": "application/json, text/plain, */*",
            "Referer": f"https://{source}.vn/",
            "Authorization": auth_token,
        }
    )  # type: ignore
    logger.debug("HTTP Session successfully initialized with required headers.")
    return s
