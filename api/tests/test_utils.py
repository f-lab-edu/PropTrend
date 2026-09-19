"""갱신 단위 좌표 검증 함수의 경계값."""

from datetime import UTC, date, datetime, timedelta

import pytest

from src.jobs.utils import month_range, parse_deal_ymd, split_sgg_cd, today_kst


def test_parse_deal_ymd_splits_year_and_month() -> None:
    assert parse_deal_ymd("202302") == ("2023", "02")


@pytest.mark.parametrize(
    "deal_ymd",
    [
        "2023",  # 짧다
        "2023021",  # 길다
        "2023-2",  # 숫자가 아니다
        "202300",  # 월이 00
        "202313",  # 월이 13
        "２０２３０２",  # 전각 숫자
        "٢٠٢٣٠٢",  # 아라비아-인도 숫자. isdigit()만으로는 통과한다
    ],
)
def test_parse_deal_ymd_rejects_malformed(deal_ymd: str) -> None:
    with pytest.raises(ValueError):
        parse_deal_ymd(deal_ymd)


@pytest.mark.parametrize(
    ("deal_ymd", "expected"),
    [
        ("202302", (date(2023, 2, 1), date(2023, 3, 1))),
        ("202312", (date(2023, 12, 1), date(2024, 1, 1))),  # 이듬해로 넘어간다
    ],
)
def test_month_range_returns_half_open_interval(deal_ymd: str, expected: tuple[date, date]) -> None:
    assert month_range(deal_ymd) == expected


def test_split_sgg_cd_splits_sido_and_sigungu() -> None:
    assert split_sgg_cd("11110") == ("11", "110")


@pytest.mark.parametrize("sgg_cd", ["1111", "111100", "1111a", "１１１１０"])
def test_split_sgg_cd_rejects_malformed(sgg_cd: str) -> None:
    with pytest.raises(ValueError):
        split_sgg_cd(sgg_cd)


def test_today_kst_is_ahead_of_utc() -> None:
    # KST는 UTC보다 9시간 빠르므로 날짜가 같거나 하루 앞선다.
    assert today_kst() - datetime.now(UTC).date() in (timedelta(0), timedelta(days=1))
