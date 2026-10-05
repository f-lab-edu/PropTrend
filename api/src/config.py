"""API 서버 환경변수 설정"""

from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """API 서버가 쓰는 환경변수를 기동 시점에 읽고 검증한다."""

    # .env는 배치 작업과 함께 쓰므로 이 클래스에 없는 키는 무시한다.
    # DATABASE_URL은 배치 작업도 src/db.py로 읽으므로 API 서버 전용인 이곳에 두지 않는다.
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # 웹 페이지 정적 파일 경로. 기본값은 프로젝트 루트의 public/이고, 컨테이너에서는 PUBLIC_DIR로 덮어쓴다.
    public_dir: Path = Path(__file__).resolve().parents[2] / "public"


@lru_cache
def get_settings() -> Settings:
    """설정 인스턴스를 한 번만 만들어 재사용한다."""
    return Settings()
