"""마이그레이션 리비전이 모델 정의와 맞는지 확인한다."""

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import Connection
from sqlalchemy.ext.asyncio import AsyncEngine

pytestmark = pytest.mark.asyncio(loop_scope="session")


async def test_migrations_match_models(test_engine: AsyncEngine, alembic_cfg: Config) -> None:
    """모델을 바꾸고 리비전을 만들지 않았으면 실패한다."""

    def check(connection: Connection) -> None:
        alembic_cfg.attributes["connection"] = connection
        command.check(alembic_cfg)

    async with test_engine.connect() as conn:
        await conn.run_sync(check)
