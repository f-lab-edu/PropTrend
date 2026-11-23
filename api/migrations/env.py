"""alembic이 모델 메타데이터를 기준으로 마이그레이션을 실행하는 환경."""

import asyncio
import os
from logging.config import fileConfig

from alembic import context
from dotenv import load_dotenv
from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import create_async_engine

from src.db import DEFAULT_DATABASE_URL
from src.model import Base

config = context.config

# 테스트처럼 코드에서 부를 때는 로깅 설정을 호출한 쪽에 맡긴다.
if config.config_file_name is not None and config.attributes.get("configure_logger", True):
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def run_migrations_offline() -> None:
    """DB에 접속하지 않고 SQL 스크립트를 출력한다."""
    context.configure(
        url=os.environ.get("DATABASE_URL", DEFAULT_DATABASE_URL),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection: Connection) -> None:
    """주어진 커넥션으로 마이그레이션을 실행한다."""
    context.configure(connection=connection, target_metadata=target_metadata)
    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    """앱과 같은 접속 주소로 엔진을 만들어 마이그레이션을 실행한다."""
    # 앱은 .env에서 DATABASE_URL을 읽는다. 이미 설정된 환경변수는 덮어쓰지 않는다.
    load_dotenv()
    # 한 번 돌고 끝나는 프로세스라 커넥션을 풀에 남기지 않는다.
    engine = create_async_engine(os.environ.get("DATABASE_URL", DEFAULT_DATABASE_URL), poolclass=pool.NullPool)
    async with engine.connect() as connection:
        await connection.run_sync(do_run_migrations)
    await engine.dispose()


if context.is_offline_mode():
    run_migrations_offline()
elif (connection := config.attributes.get("connection")) is not None:
    # 테스트는 이벤트 루프 안에서 부르므로 asyncio.run을 쓸 수 없어, 열어 둔 커넥션을 넘겨받는다.
    do_run_migrations(connection)
else:
    asyncio.run(run_async_migrations())
