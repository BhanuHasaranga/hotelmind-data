"""
Centralised configuration for the HotelMind Data Engineering platform.

All values are read from environment variables (or a .env file).
Use `from config.settings import settings` anywhere in the project.
"""

from pydantic import computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # ── Source (Operational) Database ─────────────────────────────────────────
    SOURCE_DB_HOST: str = "localhost"
    SOURCE_DB_PORT: int = 5432
    SOURCE_DB_NAME: str = "hotelmind_db"
    SOURCE_DB_USER: str = "hotelmind"
    SOURCE_DB_PASSWORD: str = "hotelmind_secret"

    @computed_field  # type: ignore[misc]
    @property
    def SOURCE_DB_URL(self) -> str:
        return (
            f"postgresql://{self.SOURCE_DB_USER}:{self.SOURCE_DB_PASSWORD}"
            f"@{self.SOURCE_DB_HOST}:{self.SOURCE_DB_PORT}/{self.SOURCE_DB_NAME}"
        )

    # ── Warehouse (Analytical) Database ───────────────────────────────────────
    WAREHOUSE_DB_HOST: str = "localhost"
    WAREHOUSE_DB_PORT: int = 5433
    WAREHOUSE_DB_NAME: str = "hotelmind_warehouse"
    WAREHOUSE_DB_USER: str = "hotelmind_dw"
    WAREHOUSE_DB_PASSWORD: str = "hotelmind_dw_secret"

    @computed_field  # type: ignore[misc]
    @property
    def WAREHOUSE_DB_URL(self) -> str:
        return (
            f"postgresql://{self.WAREHOUSE_DB_USER}:{self.WAREHOUSE_DB_PASSWORD}"
            f"@{self.WAREHOUSE_DB_HOST}:{self.WAREHOUSE_DB_PORT}/{self.WAREHOUSE_DB_NAME}"
        )

    # ── MinIO / S3 Data Lake ──────────────────────────────────────────────────
    S3_ENDPOINT_URL: str = "http://localhost:9000"
    S3_ACCESS_KEY: str = "hotelmind_minio"
    S3_SECRET_KEY: str = "hotelmind_minio_secret"
    S3_BUCKET_RAW: str = "hotelmind-raw"
    S3_BUCKET_PROCESSED: str = "hotelmind-processed"

    # ── dbt ───────────────────────────────────────────────────────────────────
    DBT_PROFILES_DIR: str = "./dbt"
    DBT_PROJECT_DIR: str = "./dbt"
    DBT_TARGET: str = "dev"

    # ── Pipeline behaviour ────────────────────────────────────────────────────
    # How many days back to look when no watermark exists for a table
    ETL_INITIAL_LOOKBACK_DAYS: int = 365
    LOG_LEVEL: str = "INFO"


settings = Settings()
