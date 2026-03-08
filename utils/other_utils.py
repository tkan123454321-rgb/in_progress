import re
from schema.producer_schema import BaseMetadata
from dotenv import load_dotenv
from pathlib import Path
import requests
import os
from dotenv import load_dotenv
import yaml # type: ignore
from utils.logger_config import setup_logger
from pydantic import BaseModel, ValidationError
import requests
from typing import Any, Callable, ClassVar, Dict
logger = setup_logger(component="utils")


class ConfigLoader:
    # Biến lớp dùng chung cho mọi lần gọi
    CONFIG_PATH: ClassVar[Path] = Path(__file__).resolve().parent.parent / "config" / "metadata.yaml"

    @staticmethod
    def _process_env(content: str) -> str:
        """Xử lý biến môi trường mà không cần self."""
        load_dotenv()
        return re.sub(r'\$\{(.+?)\}', lambda m: os.getenv(m.group(1), m.group(0)), content)

    @classmethod
    def load[T: BaseMetadata](cls, model_cls: type[T]) -> T:
        """
        Dùng @classmethod để có thể gọi trực tiếp từ ConfigLoader.load().
        """
        if not cls.CONFIG_PATH.exists():
            raise FileNotFoundError(f"Không tìm thấy file: {cls.CONFIG_PATH}")

        content = cls.CONFIG_PATH.read_text(encoding="utf-8")
        processed_content = cls._process_env(content)
        raw_data = yaml.safe_load(processed_content) or {}

        # Lấy YAML_PATH từ Model để tìm ngách dữ liệu
        path_keys = getattr(model_cls, "YAML_PATH", [])
        target_dict = raw_data
        for key in path_keys:
            target_dict = target_dict.get(key, {})
        try:
            return model_cls.model_validate(target_dict)
        except ValidationError as e:
            logger.error(f"Validation error for {model_cls.__name__}: {e}")
            raise
    
# tạo 1 session duy trì kết nối  
def _get_session() -> requests.Session:
    s = requests.Session()
    # lấy các biến từ .env
    auth_token = os.getenv('AUTH_TOKEN')
    user_agent = os.getenv('USER_AGENT')
    if not auth_token or not user_agent:
        logger.critical("THIẾU CONFIG! Kiểm tra lại file .env")
    s.headers.update({
    "User-Agent": user_agent,
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://fireant.vn/",
    "Authorization": auth_token
    }) # type: ignore
    return s
