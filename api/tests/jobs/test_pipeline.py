"""갱신 단위의 단계 순서와 실패 라우팅."""

import logging
from collections.abc import AsyncGenerator, Sequence
from contextlib import asynccontextmanager
from datetime import date
from typing import Any

import pytest

from src.jobs import pipeline
from src.jobs.collector import DailyLimitReachedError
from src.jobs.pipeline import (
    PIPELINES,
    SPEC_BY_API_ID,
    MissingBronzeError,
    SaleSpec,
    SilverStageError,
    UnitResult,
    iter_units,
    recent_months,
)
from src.jobs.state import MAX_ATTEMPTS, UnitRecord
from src.model import RTMS_KNOWN_FIELDS, PropertyType, UnitStatus
from tests.jobs.conftest import FakeSession

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
        # api_url -> 그 스펙의 수집이 낼 예외. 시군구가 아니라 스펙 축으로 실패를 가른다.
        self.spec_errors: dict[str, Exception] = {}
        # lawd_cd -> 정제 단계가 앞으로 실패할 횟수. 1이면 1차만 실패하고 2차 패스에서 산다.
        self.silver_failures: dict[str, int] = {}
        # bronze를 비워 돌려줄 lawd_cd. 정제 단독 실행의 안전장치를 건드리는 데 쓴다.
        self.empty_bronze: set[str] = set()
        # 상태 표에 실제로 나간 쓰기. (연산, lawd_cd, 상태) 순이다.
        self.state_writes: list[tuple[str, str, UnitStatus | None]] = []
        # 상태 표 쓰기 자체가 터지는 상황. mark_failed에만 건다.
        self.state_write_error: Exception | None = None
        # 상태 표에 이미 남아 있다고 칠 행.
        self.leftovers: list[UnitRecord] = []
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

    def fail_spec(self, api_url: str) -> None:
        if error := self.spec_errors.get(api_url):
            raise error

    def fail_silver(self, lawd_cd: str) -> None:
        if remaining := self.silver_failures.get(lawd_cd, 0):
            self.silver_failures[lawd_cd] = remaining - 1
            raise ValueError("전처리 실패")

    def state_ops(self, lawd_cd: str) -> list[str]:
        return [operation for operation, code, _ in self.state_writes if code == lawd_cd]


def _fake_state_store(harness: Harness) -> type:
    """상태 표 대역. 어느 연산이 어느 트랜잭션에서 나갔는지만 기록한다."""

    class RefreshUnitStateStore:
        def __init__(self, session: FakeSession) -> None:
            pass

        async def mark_collected(self, api_id: str, lawd_cd: str, deal_ymd: str) -> None:
            harness.record("mark_collected", lawd_cd)
            harness.state_writes.append(("mark_collected", lawd_cd, UnitStatus.COLLECTED))

        async def clear(self, api_id: str, lawd_cd: str, deal_ymd: str) -> None:
            harness.record("clear_state", lawd_cd)
            harness.state_writes.append(("clear", lawd_cd, None))

        async def mark_failed(
            self,
            api_id: str,
            lawd_cd: str,
            deal_ymd: str,
            status: UnitStatus,
            error: BaseException,
        ) -> None:
            if harness.state_write_error:
                raise harness.state_write_error
            harness.state_writes.append(("mark_failed", lawd_cd, status))

        async def pending(self, statuses: Sequence[UnitStatus], max_attempts: int = 10) -> list[UnitRecord]:
            return [record for record in harness.leftovers if record.status in statuses]

        async def leftovers(self) -> list[UnitRecord]:
            return list(harness.leftovers)

    return RefreshUnitStateStore


def _fake_silver(harness: Harness) -> dict[str, Any]:
    """정제 단계 대역. 스펙이 ClassVar로 들고 있는 세 가지다."""

    class Preprocessor:
        def __init__(self, property_type: PropertyType, api_id: str, building_name_field: str | None) -> None:
            pass

        def preprocess(self, rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
            # 응답 0건인 구간은 좌표를 실어 보낼 행이 없다. 실제 전처리기도 빈 목록을 낸다.
            lawd_cd = rows[0]["payload"]["lawd_cd"] if rows else ""
            harness.record("preprocess", lawd_cd)
            if lawd_cd:
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
            harness.record("load_silver", rows[0]["lawd_cd"] if rows else "")
            return len(rows)

    return {"preprocessor": Preprocessor, "cleaner": TransactionCleaner, "loader": TransactionLoader}


def _fakes(harness: Harness) -> dict[str, Any]:
    """파이프라인이 이름으로 들고 있는 협력자들의 대역."""

    class RtmsDataCollector:
        def __init__(self, api_url: str, lawd_cd: str, deal_ymd: str) -> None:
            self.api_url = api_url
            self.lawd_cd = lawd_cd

        async def collect(self) -> list[dict[str, str]]:
            harness.record("collect_api", self.lawd_cd)
            harness.fail_spec(self.api_url)
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

    class ComplexLoader:
        def __init__(self, session: FakeSession) -> None:
            pass

        async def load(self, property_type: PropertyType, rows: list[dict[str, Any]]) -> int:
            harness.record("load_complex", rows[0]["lawd_cd"] if rows else "")
            return len(rows)

    class RTMSRawItemCollector:
        def __init__(self, session: FakeSession, api_id: str) -> None:
            pass

        async def collect(self, deal_ymd: str, lawd_cd: str | None = None) -> list[dict[str, Any]]:
            harness.record("read_bronze", lawd_cd or "")
            if lawd_cd in harness.empty_bronze:
                return []
            # 뒤이은 단계가 좌표를 알 수 있도록 payload에 실어 보낸다.
            return [{"id": 1, "payload": {"lawd_cd": lawd_cd}}] * BRONZE_ROWS

    return {
        "RtmsDataCollector": RtmsDataCollector,
        "RTMSRawItemCleaner": RTMSRawItemCleaner,
        "RTMSRawItemLoader": RTMSRawItemLoader,
        "RTMSRawItemCollector": RTMSRawItemCollector,
        "ComplexLoader": ComplexLoader,
        "RefreshUnitStateStore": _fake_state_store(harness),
    } | _fake_silver(harness)


@asynccontextmanager
async def _fake_session_scope() -> AsyncGenerator[FakeSession]:
    yield FakeSession()


@pytest.fixture
def harness(monkeypatch: pytest.MonkeyPatch) -> Harness:
    """파이프라인을 DB·오픈API 없이 돌릴 수 있게 만든다."""
    instance = Harness()
    fakes = _fakes(instance)

    monkeypatch.setattr(pipeline, "session_scope", _fake_session_scope)
    for name in (
        "RtmsDataCollector",
        "RTMSRawItemCleaner",
        "RTMSRawItemLoader",
        "RTMSRawItemCollector",
        "ComplexLoader",
        "RefreshUnitStateStore",
    ):
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

    # mark_collected는 bronze 적재와 같은 트랜잭션 안, clear_state는 실버 적재와 같은
    # 트랜잭션 안이어야 한다. 이 순서가 곧 "기록이 올바른 트랜잭션에 있다"의 명세다.
    assert harness.stages("11110") == [
        "collect_api",
        "clean_bronze",
        "load_bronze",
        "mark_collected",
        "read_bronze",
        "preprocess",
        "clean_silver",
        "load_silver",
        "load_complex",
        "clear_state",
    ]
    assert result == UnitResult("테스트 매매", "11110", "202602", RAW_DELETED, API_ITEMS, SILVER_DELETED, BRONZE_ROWS)


async def test_process_unit_skips_complex_for_non_complex_types(harness: Harness) -> None:
    # 연립다세대·단독다가구는 단지를 만들지 않는다.
    spec = SaleSpec(
        "테스트 연립다세대 매매", PropertyType.ROW_HOUSE, "https://api.test/rtms", "multiflex_sale", "mhouseNm"
    )

    await pipeline.process_unit(spec, "11110", "202602")

    assert "load_silver" in harness.stages("11110")
    assert "load_complex" not in harness.stages("11110")


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


async def test_silver_failure_leaves_the_collected_row(harness: Harness) -> None:
    harness.silver_failures[SIGUNGU[0]] = 2

    await pipeline.refresh_all(months=1, concurrency=1)

    # 성공이 행을 지우는 구조라, 정제가 끝나지 않은 단위는 clear 없이 COLLECTED로 남는다.
    assert harness.state_ops(SIGUNGU[0]) == ["mark_collected", "mark_failed"]
    assert ("mark_failed", SIGUNGU[0], UnitStatus.COLLECTED) in harness.state_writes


async def test_bronze_failure_is_recorded_as_failed(harness: Harness) -> None:
    harness.api_errors[SIGUNGU[0]] = RuntimeError("일시 오류")

    await pipeline.refresh_all(months=1, concurrency=1)

    # 아무것도 커밋되지 않았으므로 수집부터 다시 해야 한다.
    assert harness.state_ops(SIGUNGU[0]) == ["mark_failed"]
    assert ("mark_failed", SIGUNGU[0], UnitStatus.FAILED) in harness.state_writes


async def test_daily_limit_records_nothing(harness: Harness) -> None:
    harness.api_errors[SIGUNGU[0]] = DailyLimitReachedError("일일 제한")

    await pipeline.refresh_all(months=1, concurrency=1)

    # 제한은 실패가 아니라 정상 경로다. 남길 것이 없다.
    assert harness.state_writes == []


async def test_state_write_failure_does_not_mask_the_original_error(
    harness: Harness, caplog: pytest.LogCaptureFixture
) -> None:
    harness.api_errors[SIGUNGU[0]] = RuntimeError("원래 원인")
    harness.state_write_error = OSError("상태 표 접속 실패")

    with caplog.at_level(logging.ERROR, logger="src.jobs.pipeline"):
        summary = await pipeline.refresh_all(months=1, concurrency=1)

    # 기록에 실패했다고 회차가 뒤집히거나 원래 원인이 묻히면 안 된다.
    assert summary["failed"] == 1
    assert summary["succeeded"] == 1
    logged = [record.exc_info[0] for record in caplog.records if record.exc_info]
    assert RuntimeError in logged


async def test_refresh_units_runs_silver_only_by_default(harness: Harness) -> None:
    summary = await pipeline.refresh_units([(harness.spec, "11110", "202602")], concurrency=1)

    assert harness.stages("11110") == [
        "read_bronze",
        "preprocess",
        "clean_silver",
        "load_silver",
        "load_complex",
        "clear_state",
    ]
    assert harness.count("collect_api") == 0
    assert (summary["succeeded"], summary["silver_recovered"]) == (1, 1)


async def test_refresh_units_with_collect_calls_the_api(harness: Harness) -> None:
    summary = await pipeline.refresh_units([(harness.spec, "11110", "202602")], concurrency=1, with_collect=True)

    assert harness.count("collect_api") == 1
    assert summary["succeeded"] == 1


async def test_refresh_units_reports_a_repeated_silver_failure(harness: Harness) -> None:
    harness.silver_failures["11110"] = 1

    summary = await pipeline.refresh_units([(harness.spec, "11110", "202602")], concurrency=1)

    # 지정 재실행은 2차 패스를 붙이지 않는다. 한 번 터지면 그대로 실패다.
    assert (summary["silver_failed"], summary["succeeded"]) == (1, 0)


async def test_empty_bronze_does_not_wipe_silver(harness: Harness) -> None:
    harness.empty_bronze.add("11110")

    summary = await pipeline.refresh_units([(harness.spec, "11110", "202602")], concurrency=1)

    # 여기서 멈추지 않으면 정리기가 구간을 지우고 적재기가 0건을 넣어 실버가 조용히 빈다.
    assert harness.stages("11110") == ["read_bronze"]
    assert (summary["no_bronze"], summary["silver_failed"]) == (1, 0)


async def test_refresh_all_allows_empty_bronze(harness: Harness) -> None:
    harness.empty_bronze.update(SIGUNGU)

    summary = await pipeline.refresh_all(months=1, concurrency=1)

    # 회차 안에서는 응답 0건이 곧 빈 구간이라 삭제가 정상이다. 안전장치는 지정 재실행 전용이다.
    assert (summary["no_bronze"], summary["succeeded"]) == (0, 2)
    assert "clean_silver" in harness.stages(SIGUNGU[0])


async def test_process_unit_requires_bronze_only_when_asked(harness: Harness) -> None:
    harness.empty_bronze.add("11110")

    with pytest.raises(MissingBronzeError, match="bronze에 응답이 없다"):
        await pipeline.process_unit(harness.spec, "11110", "202602", require_bronze=True)


async def test_failed_units_needs_with_collect_for_bronze_failures(harness: Harness) -> None:
    harness.leftovers = [
        UnitRecord("apart_sale", "11110", "202602", UnitStatus.COLLECTED, 1),
        UnitRecord("apart_sale", "11140", "202602", UnitStatus.FAILED, 1),
    ]

    silver_only = await pipeline.failed_units(with_collect=False)
    everything = await pipeline.failed_units(with_collect=True)

    # 수집이 필요한 단위는 정제만 돌리는 기본 모드에서 빠진다.
    assert [lawd_cd for _, lawd_cd, _ in silver_only] == ["11110"]
    assert [lawd_cd for _, lawd_cd, _ in everything] == ["11110", "11140"]


async def test_failed_units_skips_unknown_api_ids(harness: Harness) -> None:
    harness.leftovers = [UnitRecord("사라진_슬러그", "11110", "202602", UnitStatus.COLLECTED, 1)]

    # 슬러그가 바뀌면 해석되지 않는 행이 남는다. 조용히 지우지도, 터지지도 않아야 한다.
    assert await pipeline.failed_units(with_collect=False) == []


async def test_summary_carries_the_leftover_counts(harness: Harness) -> None:
    harness.leftovers = [
        UnitRecord("apart_sale", "11110", "202602", UnitStatus.COLLECTED, 1),
        UnitRecord("apart_sale", "11140", "202602", UnitStatus.COLLECTED, MAX_ATTEMPTS),
    ]

    summary = await pipeline.refresh_all(months=1, concurrency=1)

    # 종료 코드가 0인 회차에서도 이 두 값이 "봐야 한다"는 신호가 된다.
    assert (summary["pending_units"], summary["stuck_units"]) == (2, 1)


SECOND_SPEC = SaleSpec(
    "테스트 오피스텔 매매", PropertyType.OFFICETEL, "https://api.test/offi", "officetel_sale", "offiNm"
)


def _two_specs(monkeypatch: pytest.MonkeyPatch, harness: Harness) -> None:
    """조합을 둘로 늘려 API별 분해가 실제로 갈리는지 볼 수 있게 한다."""
    monkeypatch.setattr(pipeline, "PIPELINES", (harness.spec, SECOND_SPEC))


def _api_summary(caplog: pytest.LogCaptureFixture) -> dict[str, dict[str, int]]:
    """회차 끝에 한 번 나가는 API별 집계 레코드."""
    records = [record for record in caplog.records if getattr(record, "stage", None) == "api_summary"]
    assert len(records) == 1
    return records[0].by_api


async def test_api_summary_splits_counters_per_spec(
    harness: Harness, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    _two_specs(monkeypatch, harness)

    with caplog.at_level(logging.INFO, logger="src.jobs.pipeline"):
        await pipeline.refresh_all(months=1, concurrency=1)

    # 시군구 2 × 1개월 × 2종 = 4단위가 스펙별로 2건씩 갈려야 한다.
    assert _api_summary(caplog) == {
        "apart_sale": {"units": 2, "succeeded": 2, "loaded": 2 * BRONZE_ROWS},
        "officetel_sale": {"units": 2, "succeeded": 2, "loaded": 2 * BRONZE_ROWS},
    }


async def test_api_summary_blames_only_the_failing_spec(
    harness: Harness, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    _two_specs(monkeypatch, harness)
    harness.spec_errors[SECOND_SPEC.api_url] = RuntimeError("일시 오류")

    with caplog.at_level(logging.INFO, logger="src.jobs.pipeline"):
        summary = await pipeline.refresh_all(months=1, concurrency=1)

    by_api = _api_summary(caplog)
    # 합계만 보면 "4단위 중 2건 실패"로 끝난다. 어느 API인지는 분해에만 있다.
    assert summary["failed"] == 2
    assert by_api["officetel_sale"]["failed"] == 2
    assert "failed" not in by_api["apart_sale"]
    assert by_api["apart_sale"]["succeeded"] == 2


async def test_api_summary_totals_match_the_summary(
    harness: Harness, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    _two_specs(monkeypatch, harness)
    harness.spec_errors[SECOND_SPEC.api_url] = RuntimeError("일시 오류")
    harness.silver_failures[SIGUNGU[0]] = 1

    with caplog.at_level(logging.INFO, logger="src.jobs.pipeline"):
        summary = await pipeline.refresh_all(months=1, concurrency=1)

    by_api = _api_summary(caplog)
    # bump 하나로 합계와 분해를 함께 올리는 이유다. 따로 올리면 언젠가 여기서 어긋난다.
    for key in ("units", "succeeded", "failed", "silver_failed", "silver_recovered", "skipped", "loaded"):
        assert sum(counters.get(key, 0) for counters in by_api.values()) == summary[key], key


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
    # 명령줄 좌표를 스펙으로 되돌리는 유일한 경로라, 빠진 슬러그는 곧 못 돌리는 단위다.
    assert set(SPEC_BY_API_ID) == set(api_ids)
    for spec in PIPELINES:
        # api_id는 rtms_raw_items에 그대로 들어가는 슬러그다. 오타는 삭제가 못 찾는 행을 만든다.
        assert spec.api_id in RTMS_KNOWN_FIELDS
        if spec.building_name_field is not None:
            assert spec.building_name_field in RTMS_KNOWN_FIELDS[spec.api_id]
