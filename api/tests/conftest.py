from collections.abc import AsyncGenerator

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import JSON, Integer, MetaData, Table
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.db import get_db_session
from app.main import app
from app.model.prop_transaction import (
    LegalStandardCode,
    RentTransaction,
    SaleTransaction,
)
from app.model.user import User


def _sqlite_metadata() -> MetaData:
    metadata = MetaData()
    tables = [
        model.__table__.to_metadata(metadata)
        for model in (User, SaleTransaction, RentTransaction, LegalStandardCode)
    ]
    for table in tables:
        # SQLite only grants rowid-alias autoincrement to a PK column declared as
        # exactly INTEGER, so the models' BigInteger id is swapped for tests.
        if "id" in table.c:
            table.c.id.type = Integer()
    _swap_jsonb_columns(metadata)
    return metadata


def _swap_jsonb_columns(metadata: MetaData) -> None:
    # SQLite cannot render PostgreSQL's JSONB, so the generic JSON type is used
    # for tests. Both store the same dict values through SQLAlchemy.
    rent_table: Table = metadata.tables[RentTransaction.__tablename__]
    rent_table.c.road_address_detail.type = JSON()


@pytest.fixture
async def session_factory() -> AsyncGenerator[async_sessionmaker]:
    metadata = _sqlite_metadata()

    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    yield factory
    await engine.dispose()


@pytest.fixture
async def client(session_factory: async_sessionmaker) -> AsyncGenerator[AsyncClient]:
    async def override_get_db_session() -> AsyncGenerator:
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_db_session] = override_get_db_session
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac
    app.dependency_overrides.clear()
