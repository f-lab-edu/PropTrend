"""갱신 단위의 단계 순서와 실패 라우팅."""

from collections.abc import AsyncGenerator, Sequence
from contextlib import asynccontextmanager
from datetime import date
from typing import Any

import pytest
from conftest import FakeSession

from src.jobs import pipeline
from src.jobs.collector import DailyLimitReachedError
from src.jobs.pipeline import (
    PIPELINES,
    SaleSpec,
    SilverStageError,
    UnitResult,
    iter_units,
    recent_months,
)
from src.model import RTMS_KNOWN_FIELDS, PropertyType

# 단계별로 다른 값을 돌려줘야 UnitResult가 어느 단계의 결과를 어디에 담는지 확인할 수 있다.
API_ITEMS = 2
RAW_DELETED = 3
BRONZE_ROWS = 1
SILVER_DELETED = 5

SIGUNGU = ("11110", "11140")


class Harness:
    """파이프라인 바깥 경계(세션·수집기·정리기·적재기)를 전부 가짜로 바꾸고 호출을 기록한다."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []
        self.api_errors: dict[str, Exception] = {}
        # lawd_cd -> 정제 단계가 앞으로 실패할 횟수. 1이면 1차만 실패하고 2차 패스에서 산다.
        self.silver_failures: dict[str, int] = {}
        self.spec = SaleSpec("테스트 매매", PropertyType.APT, "https://api.test/rtms", "apart_sale", "aptNm")

    def record(self, stage: str, lawd_cd: str) -> None:
        self.calls.append((stage, lawd_cd))

    def stages(self, lawd_cd: str) -> list[str]:
        return [stage for stage, code in self.calls if code == lawd_cd]

    def count(self, stage: str) -> int:
        return sum(1 for recorded, _ in self.calls if recorded == stage)

    def fail_api(self, lawd_cd: str) -> None:
        if error := self.api_errors.get(lawd_cd):
            raise error

    def fail_silver(self, lawd_cd: str) -> None:
        if remaining := self.silver_failures.get(lawd_cd, 0):
            self.silver_failures[lawd_cd] = remaining - 1
            raise ValueError("전처리 실패")


def _fakes(harness: Harness) -> dict[str, Any]:
    """파이프라인이 이름으로 들고 있는 협력자들의 대역."""

    class RtmsDataCollector:
        def __init__(self, api_url: str, lawd_cd: str, deal_ymd: str) -> None:
            self.lawd_cd = lawd_cd

        async def collect(self) -> list[dict[str, str]]:
            harness.record("collect_api", self.lawd_cd)
            harness.fail_api(self.lawd_cd)
            return [{"aptNm": "청운현대"}] * API_ITEMS

    class RTMSRawItemCleaner:
        def __init__(self, session: FakeSession, api_id: str) -> None:
            pass

        async def clean(self, deal_ymd: str, lawd_cd: str) -> int:
            harness.record("clean_bronze", lawd_cd)
            return RAW_DELETED

    class RTMSRawItemLoader:
        def __init__(self, session: FakeSession, api_id: str) -> None:
            pass

        async def load(self, lawd_cd: str, deal_ymd: str, items: list[dict[str, Any]]) -> int:
            harness.record("load_bronze", lawd_cd)
            return len(items)

    class RTMSRawItemCollector:
        def __init__(self, session: FakeSession, api_id: str) -> None:
            pass

        async def collect(self, deal_ymd: str, lawd_cd: str | None = None) -> list[dict[str, Any]]:
            harness.record("read_bronze", lawd_cd or "")
            # 뒤이은 단계가 좌표를 알 수 있도록 payload에 실어 보낸다.
            return [{"id": 1, "payload": {"lawd_cd": lawd_cd}}] * BRONZE_ROWS

    class Preprocessor:
        def __init__(self, property_type: PropertyType, api_id: str, building_name_field: str | None) -> None:
            pass

        def preprocess(self, rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
            lawd_cd = rows[0]["payload"]["lawd_cd"]
            harness.record("preprocess", lawd_cd)
            harness.fail_silver(lawd_cd)
            return [{"lawd_cd": lawd_cd} for _ in rows]

    class TransactionCleaner:
        def __init__(self, session: FakeSession) -> None:
            pass

        async def clean(self, property_type: PropertyType, deal_ymd: str, sgg_cd: str | None = None) -> int:
            harness.record("clean_silver", sgg_cd or "")
            return SILVER_DELETED

    class TransactionLoader:
        def __init__(self, session: FakeSession) -> None:
            pass

        async def load(self, rows: list[dict[str, Any]]) -> int:
            harness.record("load_silver", rows[0]["lawd_cd"])
            return len(rows)

    return {
        "RtmsDataCollector": RtmsDataCollector,
        "RTMSRawItemCleaner": RTMSRawItemCleaner,
        "RTMSRawItemLoader": RTMSRawItemLoader,
        "RTMSRawItemCollector": RTMSRawItemCollector,
        "preprocessor": Preprocessor,
        "cleaner": TransactionCleaner,
        "loader": TransactionLoader,
    }


@asynccontextmanager
async def _fake_session_scope() -> AsyncGenerator[FakeSession]:
    yield FakeSession()


@pytest.fixture
def harness(monkeypatch: pytest.MonkeyPatch) -> Harness:
    """파이프라인을 DB·오픈API 없이 돌릴 수 있게 만든다."""
    instance = Harness()
    fakes = _fakes(instance)

    monkeypatch.setattr(pipeline, "session_scope", _fake_session_scope)
    for name in ("RtmsDataCollector", "RTMSRawItemCleaner", "RTMSRawItemLoader", "RTMSRawItemCollector"):
        monkeypatch.setattr(pipeline, name, fakes[name])
    # 스펙의 전처리기·정리기·적재기는 ClassVar라 클래스 쪽을 갈아끼운다.
    for name in ("preprocessor", "cleaner", "loader"):
        monkeypatch.setattr(SaleSpec, name, fakes[name])

    # 조합을 하나로 줄여 단위 수를 읽을 수 있게 만든다.
    monkeypatch.setattr(pipeline, "PIPELINES", (instance.spec,))
    monkeypatch.setattr(pipeline, "refresh_legal_dong_codes", _noop_legal_dong)
    monkeypatch.setattr(pipeline, "sigungu_codes", _fake_sigungu_codes)
    return instance


async def _noop_legal_dong() -> int:
    return 0


async def _fake_sigungu_codes() -> list[str]:
    return list(SIGUNGU)


async def test_refresh_unit_runs_stages_in_order(harness: Harness) -> None:
    result = await pipeline.refresh_unit(harness.spec, "11110", "202602")

    assert harness.stages("11110") == [
        "collect_api",
        "clean_bronze",
        "load_bronze",
        "read_bronze",
        "preprocess",
        "clean_silver",
        "load_silver",
    ]
    assert result == UnitResult("테스트 매매", "11110", "202602", RAW_DELETED, API_ITEMS, SILVER_DELETED, BRONZE_ROWS)


async def test_refresh_unit_validates_deal_ymd_before_calling_api(harness: Harness) -> None:
    with pytest.raises(ValueError, match="deal_ymd"):
        await pipeline.refresh_unit(harness.spec, "11110", "2026")

    # 형식을 API 호출 전에 막지 못하면 일일 호출량만 축낸다.
    assert harness.calls == []


async def test_process_unit_wraps_failure_as_silver_stage_error(harness: Harness) -> None:
    harness.silver_failures["11110"] = 1

    with pytest.raises(SilverStageError, match="bronze는 남아 있다") as error:
        await pipeline.process_unit(harness.spec, "11110", "202602")

    assert isinstance(error.value.__cause__, ValueError)


async def test_daily_limit_skips_remaining_units(harness: Harness) -> None:
    harness.api_errors[SIGUNGU[0]] = DailyLimitReachedError("일일 제한")

    summary = await pipeline.refresh_all(months=1, concurrency=1)

    assert summary["daily_limit"] == 1
    assert summary["skipped"] == 1  # 시군구 2 × 1개월 × 1종 = 2단위
    assert summary["succeeded"] == 0
    # 남은 단위는 슬롯만 스치고 끝나야 한다.
    assert harness.count("collect_api") == 1


async def test_silver_failure_is_retried_without_recollecting(harness: Harness) -> None:
    harness.silver_failures[SIGUNGU[0]] = 1

    summary = await pipeline.refresh_all(months=1, concurrency=1)

    assert summary["silver_recovered"] == 1
    assert summary["silver_failed"] == 0
    assert summary["succeeded"] == 2  # 2차에서 살아난 단위도 성공에 포함된다
    stages = harness.stages(SIGUNGU[0])
    assert stages.count("collect_api") == 1  # 2차 패스는 오픈API를 부르지 않는다
    assert stages.count("read_bronze") == 2


async def test_repeated_silver_failure_is_not_a_run_failure(harness: Harness) -> None:
    harness.silver_failures[SIGUNGU[0]] = 2

    summary = await pipeline.refresh_all(months=1, concurrency=1)

    assert summary["silver_failed"] == 1
    # 프로세스를 다시 띄워도 결과가 같으므로 종료 코드를 올리는 failed와는 구분된다.
    assert summary["failed"] == 0
    assert summary["succeeded"] == 1


async def test_unit_failure_does_not_stop_others(harness: Harness) -> None:
    harness.api_errors[SIGUNGU[0]] = RuntimeError("일시 오류")

    summary = await pipeline.refresh_all(months=1, concurrency=1)

    assert (summary["failed"], summary["succeeded"], summary["skipped"]) == (1, 1, 0)


def test_recent_months_walks_backwards_across_the_year() -> None:
    assert recent_months(3, date(2026, 1, 15)) == ["202601", "202512", "202511"]


@pytest.mark.parametrize("months", [0, -1, 25])
def test_recent_months_rejects_out_of_range(months: int) -> None:
    # 반복 횟수를 정하는 값이라 호출부가 아니라 여기서 막아야 한다.
    with pytest.raises(ValueError, match="months는"):
        recent_months(months)


def test_iter_units_visits_newest_month_first() -> None:
    units = list(iter_units(["11110", "11140"], ["202602", "202601"]))

    assert len(units) == 2 * 2 * len(PIPELINES)
    # 중간에 잘려도 최근 데이터가 먼저 반영돼야 한다.
    assert {deal_ymd for _, _, deal_ymd in units[: 2 * len(PIPELINES)]} == {"202602"}


def test_pipeline_specs_match_bronze_baseline() -> None:
    api_ids = [spec.api_id for spec in PIPELINES]

    assert len(api_ids) == len(set(api_ids)) == len(RTMS_KNOWN_FIELDS)
    for spec in PIPELINES:
        # api_id는 rtms_raw_items에 그대로 들어가는 슬러그다. 오타는 삭제가 못 찾는 행을 만든다.
        assert spec.api_id in RTMS_KNOWN_FIELDS
        if spec.building_name_field is not None:
            assert spec.building_name_field in RTMS_KNOWN_FIELDS[spec.api_id]
