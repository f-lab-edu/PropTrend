"""API 서버 환경변수 설정"""

from functools import lru_cache
from typing import Annotated

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """API 서버가 쓰는 환경변수를 기동 시점에 읽고 검증한다."""

    # .env는 배치 작업과 함께 쓰므로 이 클래스에 없는 키는 무시한다.
    # DATABASE_URL은 배치 작업도 src/db.py로 읽으므로 API 서버 전용인 이곳에 두지 않는다.
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # X-API-KEY 헤더와 비교할 값. .env.example을 그대로 복사한 빈 값도 누락으로 본다.
    api_key: Annotated[SecretStr, Field(min_length=1)]


@lru_cache
def get_settings() -> Settings:
    """설정 인스턴스를 한 번만 만들어 재사용한다."""
    # 필수 필드는 환경변수에서 채워지므로 타입 검사기의 인자 누락 경고를 끈다.
    return Settings()  # pyright: ignore[reportCallIssue]
