"""API 테스트가 쓰는 실제 테스트 DB 엔진."""

import os
from collections.abc import AsyncIterator
from pathlib import Path

import pytest
import pytest_asyncio
from alembic import command
from alembic.config import Config
from sqlalchemy import Connection, make_url, text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

ALEMBIC_INI = Path(__file__).resolve().parents[1] / "alembic.ini"

# 실데이터가 든 prop_trend와 분리한다. 테이블을 지우고 다시 만들기 때문에 이름이 _test로 끝나야만 쓴다.
TEST_DATABASE_URL = os.environ.get(
    "TEST_DATABASE_URL", "postgresql+asyncpg://postgres:postgres@localhost:5432/prop_trend_test"
)


@pytest.fixture(scope="session")
def alembic_cfg() -> Config:
    """테스트가 코드에서 alembic 명령을 부를 때 쓰는 설정."""
    cfg = Config(ALEMBIC_INI)
    # env.py의 fileConfig가 pytest의 로그 캡처 설정을 덮어쓰지 않게 한다.
    cfg.attributes["configure_logger"] = False
    return cfg


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def test_engine(alembic_cfg: Config) -> AsyncIterator[AsyncEngine]:
    """테스트 DB가 없으면 만들고, 마이그레이션으로 스키마를 처음부터 다시 만든 엔진."""
    url = make_url(TEST_DATABASE_URL)
    if not url.database or not url.database.endswith("_test"):
        raise RuntimeError(f"테스트 DB 이름은 _test로 끝나야 합니다: {url.database}")

    admin_engine = create_async_engine(url.set(database="postgres"), isolation_level="AUTOCOMMIT")
    async with admin_engine.connect() as conn:
        exists = await conn.scalar(text("SELECT 1 FROM pg_database WHERE datname = :name"), {"name": url.database})
        if not exists:
            await conn.execute(text(f'CREATE DATABASE "{url.database}"'))
    await admin_engine.dispose()

    def upgrade(connection: Connection) -> None:
        alembic_cfg.attributes["connection"] = connection
        command.upgrade(alembic_cfg, "head")

    engine = create_async_engine(url)
    # 운영 DB와 같은 스키마에서 돌도록 create_all이 아닌 마이그레이션으로 만든다.
    # 리비전 사이에 바뀐 테이블과 alembic_version까지 남김없이 지우려고 스키마를 통째로 다시 만든다.
    async with engine.begin() as conn:
        await conn.execute(text("DROP SCHEMA public CASCADE"))
        await conn.execute(text("CREATE SCHEMA public"))
        await conn.run_sync(upgrade)
    yield engine
    await engine.dispose()
