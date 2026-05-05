import psycopg
from psycopg.rows import dict_row
from common.core.logger_config import setup_logger
import os
from typing import ClassVar

logger = setup_logger(component="infrastructure")


class PostgresClient:
    """
    Manages connections to the PostgreSQL database.
    """

    # classvars
    _USER: ClassVar[str | None] = os.getenv("POSTGRES_USER")
    _PASS: ClassVar[str | None] = os.getenv("POSTGRES_PASSWORD")

    def _build_conn_str(self, db_name: str | None = None) -> str:
        """
        Builds the connection string safely.
        """
        if not self._USER or not self._PASS:
            error_msg = "Missing POSTGRES_USER or POSTGRES_PASSWORD in .env file."
            logger.critical(error_msg)
            raise EnvironmentError(error_msg)
        return f"postgresql://{self._USER}:{self._PASS}@postgres:5432/{db_name}"

    def get_db_connection(self, db_name: str | None = None) -> psycopg.Connection:
        """
        Creates and returns a connection to Postgres.
        """
        conn_str = self._build_conn_str(db_name=db_name)
        try:
            conn = psycopg.connect(
                conninfo=conn_str,
                autocommit=True,
                row_factory=dict_row,  # type: ignore
            )
            return conn
        except psycopg.Error as e:
            logger.critical(
                f"Failed to connect to Postgres. SQL State: {e.sqlstate}, Error: {e}",
                exc_info=True,
            )
            raise


def map_trino_to_pg_type(trino_type: str) -> str:
    """
    Maps Trino data types to PostgreSQL data types.
    This acts as a translator to ensure tables created in Postgres match the structure in Trino.
    """
    # Remove extra spaces and convert to lowercase for easy matching
    t = trino_type.lower().strip()

    if t.startswith("varchar") or t.startswith("char"):
        return "TEXT"

    if t == "double":
        return "DOUBLE PRECISION"
    if t == "real":
        return "REAL"

    if t.startswith("decimal") or t.startswith("numeric"):
        return t.upper()

    if t.startswith("timestamp"):
        if "with time zone" in t:
            return "TIMESTAMP WITH TIME ZONE"
        return "TIMESTAMP"

    if t == "date":
        return "DATE"

    if t == "bigint":
        return "BIGINT"
    if t in ("integer", "int"):
        return "INTEGER"
    if t in ("smallint", "tinyint"):
        return "SMALLINT"

    if t == "boolean":
        return "BOOLEAN"

    return "TEXT"
