from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = (
        "postgresql+asyncpg://postgres:postgres@localhost:5432/prop_trend"
    )
    s3_bucket: str
    s3_expected_bucket_owner: str
    s3_prefix: str = ""
    data_go_kr_service_key: str


@lru_cache
def get_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]
