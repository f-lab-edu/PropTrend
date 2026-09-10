"""갱신 단위를 가리키는 계약년월/시군구코드의 공용 검증과 상수."""

from datetime import date, datetime
from zoneinfo import ZoneInfo

# 스케줄러의 cron과 "이번 달" 계산이 같은 기준을 써야 월말 자정 근처에서 어긋나지 않는다.
KST = ZoneInfo("Asia/Seoul")


def today_kst() -> date:
    """실거래 신고 기준 시각(KST)의 오늘 날짜."""
    return datetime.now(KST).date()


def parse_deal_ymd(deal_ymd: str) -> tuple[str, str]:
    """`"202302"`를 `("2023", "02")`로 나눈다."""
    if len(deal_ymd) != 6 or not (deal_ymd.isascii() and deal_ymd.isdigit()):
        # isdigit()만으로는 부족하다. 아라비아-인도 숫자 같은 비ASCII 숫자도 True다.
        raise ValueError(f"deal_ymd는 YYYYMM 6자리여야 한다: {deal_ymd!r}")
    if not "01" <= deal_ymd[4:] <= "12":
        raise ValueError(f"deal_ymd의 월이 01~12를 벗어난다: {deal_ymd!r}")
    return deal_ymd[:4], deal_ymd[4:]


def month_range(deal_ymd: str) -> tuple[date, date]:
    """계약년월을 `deal_date` 비교용 반개구간 [시작, 끝)으로 바꾼다."""
    year, month = (int(part) for part in parse_deal_ymd(deal_ymd))
    start = date(year, month, 1)
    end = date(year + 1, 1, 1) if month == 12 else date(year, month + 1, 1)
    return start, end


def split_sgg_cd(sgg_cd: str) -> tuple[str, str]:
    """RTMS의 5자리 `sggCd`를 시도 2자리 + 시군구 3자리로 나눈다."""
    if len(sgg_cd) != 5 or not (sgg_cd.isascii() and sgg_cd.isdigit()):
        raise ValueError(f"sgg_cd는 숫자 5자리여야 한다: {sgg_cd!r}")
    return sgg_cd[:2], sgg_cd[2:]
