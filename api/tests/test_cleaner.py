"""정리기가 조립하는 삭제 조건의 범위. 조건이 어긋나도 예외가 없어 로그로는 드러나지 않는다."""

from datetime import date
from typing import Any

import pytest
from conftest import FakeSession
from sqlalchemy import Delete
from sqlalchemy.sql.elements import BooleanClauseList

from src.jobs.cleaner import (
    RentTransactionCleaner,
    RTMSRawItemCleaner,
    SaleTransactionCleaner,
    TransactionCleaner,
)
from src.jobs.loader import RTMSRawItemLoader
from src.model import PropertyType


def where_conditions(statement: Delete) -> list[tuple[str, str, Any]]:
    """DELETE의 WHERE 절을 `(컬럼, 연산자, 값)` 목록으로 펼친다."""
    clause = statement.whereclause
    parts = clause.clauses if isinstance(clause, BooleanClauseList) else [clause]
    return [(part.left.name, part.operator.__name__, part.right.value) for part in parts]


def only_statement(session: FakeSession) -> Delete:
    """세션에 단 하나의 문장만 나갔는지 확인하고 그 문장을 돌려준다."""
    ((statement, _),) = session.statements
    return statement


@pytest.mark.parametrize(
    ("cleaner", "table"),
    [(SaleTransactionCleaner, "sale_transactions"), (RentTransactionCleaner, "rent_transactions")],
)
async def test_clean_scopes_to_type_month_and_sigungu(cleaner: type[TransactionCleaner], table: str) -> None:
    session = FakeSession()

    await cleaner(session).clean(PropertyType.APT, "202302", "11110")

    statement = only_statement(session)
    assert statement.table.name == table
    # property_type이 빠지면 아파트 갱신이 같은 표의 다른 유형까지 지운다.
    assert where_conditions(statement) == [
        ("property_type", "eq", PropertyType.APT),
        ("deal_date", "ge", date(2023, 2, 1)),
        ("deal_date", "lt", date(2023, 3, 1)),
        ("sido_code", "eq", "11"),
        ("sigungu_code", "eq", "110"),
    ]


@pytest.mark.parametrize(
    ("deal_ymd", "start", "end"),
    [("202302", date(2023, 2, 1), date(2023, 3, 1)), ("202312", date(2023, 12, 1), date(2024, 1, 1))],
)
async def test_clean_uses_half_open_month_range(deal_ymd: str, start: date, end: date) -> None:
    session = FakeSession()

    await SaleTransactionCleaner(session).clean(PropertyType.APT, deal_ymd, "11110")

    # 끝을 `<=`로 잡으면 다음 달 1일 계약이 함께 지워진다.
    date_conditions = [
        condition for condition in where_conditions(only_statement(session)) if condition[0] == "deal_date"
    ]
    assert date_conditions == [("deal_date", "ge", start), ("deal_date", "lt", end)]


async def test_clean_without_sgg_cd_deletes_nationwide() -> None:
    session = FakeSession()

    await SaleTransactionCleaner(session).clean(PropertyType.APT, "202302")

    # 기본값이라 호출부에서 인자 하나만 빠져도 그 달 전국이 날아간다. 백필 외에는 쓰지 않는다.
    assert [name for name, _, _ in where_conditions(only_statement(session))] == [
        "property_type",
        "deal_date",
        "deal_date",
    ]


async def test_clean_returns_deleted_rowcount() -> None:
    session = FakeSession(rowcount=3)

    assert await SaleTransactionCleaner(session).clean(PropertyType.APT, "202302", "11110") == 3


async def test_bronze_clean_targets_the_keys_the_loader_wrote() -> None:
    lawd_cd, deal_ymd = "11110", "202602"
    load_session = FakeSession()
    await RTMSRawItemLoader(load_session, "apart_sale").load(lawd_cd, deal_ymd, [{"aptNm": "청운현대"}])
    ((_, rows),) = load_session.statements

    clean_session = FakeSession()
    await RTMSRawItemCleaner(clean_session, "apart_sale").clean(deal_ymd, lawd_cd)

    statement = only_statement(clean_session)
    assert statement.table.name == "rtms_raw_items"
    # 적재기가 넣는 값과 모양이 어긋나면 삭제가 빈손으로 끝나고 재적재분이 그대로 중복된다.
    assert where_conditions(statement) == [
        ("api_id", "eq", rows[0]["api_id"]),
        ("deal_ymd", "eq", rows[0]["deal_ymd"]),
        ("lawd_cd", "eq", rows[0]["lawd_cd"]),
    ]


@pytest.mark.parametrize(
    ("deal_ymd", "lawd_cd"),
    [
        ("2026", "11110"),  # 계약년월이 6자리가 아니다
        ("202613", "11110"),  # 월이 13
        ("202602", "1111"),  # 시군구코드가 5자리가 아니다
        ("11110", "202602"),  # 인자 순서가 뒤바뀌었다
    ],
)
async def test_bronze_clean_rejects_malformed_unit(deal_ymd: str, lawd_cd: str) -> None:
    session = FakeSession()

    # 반환값을 버리는 검증 호출이라 죽은 코드로 보이지만, 갱신 단위가 어긋난 삭제를 막는 유일한 장치다.
    with pytest.raises(ValueError):
        await RTMSRawItemCleaner(session, "apart_sale").clean(deal_ymd, lawd_cd)

    assert session.statements == []
