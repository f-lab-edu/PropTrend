"""앱 기동 시점의 설정 검증 테스트."""

from collections.abc import Iterator
from pathlib import Path

import pytest
from pydantic import ValidationError

from src.config import get_settings
from src.main import app, lifespan


class TestLifespan:
    @pytest.fixture(autouse=True)
    def setup(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Iterator[None]:
        # 실행 위치의 .env가 API_KEY를 채우지 않도록 빈 디렉터리에서 돌린다.
        monkeypatch.chdir(tmp_path)
        get_settings.cache_clear()
        yield
        get_settings.cache_clear()

    async def test_fails_without_api_key(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("API_KEY", raising=False)

        with pytest.raises(ValidationError, match="api_key"):
            async with lifespan(app):
                pass

    async def test_fails_with_empty_api_key(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("API_KEY", "")

        with pytest.raises(ValidationError, match="api_key"):
            async with lifespan(app):
                pass
