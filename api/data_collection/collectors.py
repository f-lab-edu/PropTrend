import asyncio
import re
import xml.etree.ElementTree as ET
from abc import ABC, abstractmethod
from datetime import date, datetime, timedelta, timezone
from typing import ClassVar

import httpx
from defusedxml.ElementTree import fromstring as safe_xml_fromstring
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.model.prop_transaction import LegalStandardCode

# 수집 대상 기간의 기본값(개월). 호출자가 필요에 따라 더 짧은 기간을 지정할 수 있다.
COLLECTION_MONTHS = 24

MAX_ROWS_PER_PAGE = 10000
REQUEST_DELAY_SECONDS = 0.1
MAX_TRANSIENT_RETRIES = 3
RETRY_BACKOFF_SECONDS = 2.0
REQUEST_TIMEOUT_SECONDS = 10.0

SUCCESS_RESULT_CODE = "000"
NO_DATA_RESULT_CODE = "03"
DAILY_LIMIT_RESULT_CODE = "22"
TRANSIENT_RESULT_CODES = {"01", "02", "04", "05"}

LAWD_CD_PATTERN = re.compile(r"^\d{5}$")

KST = timezone(timedelta(hours=9))


class DailyLimitReachedError(Exception):
    pass


class FatalApiError(Exception):
    pass


class RegionCodesNotFoundError(Exception):
    pass


async def _load_region_codes(
    session_factory: async_sessionmaker[AsyncSession],
) -> list[str]:
    # 실거래가 수집 대상 시군구는 법정동코드 마스터에서 가져온다. 마스터를 먼저 갱신해
    # 두면 행정구역이 개편돼도 같은 실행 안에서 새 시군구까지 수집된다.
    async with session_factory() as session:
        result = await session.scalars(
            select(LegalStandardCode.code).order_by(LegalStandardCode.code)
        )
        rows = result.all()

    if not rows:
        # 코드가 하나도 없으면 아무 요청도 보내지 않아 "0건 수집"으로 조용히 끝난다.
        # 수집 실패와 데이터 없음을 구분하기 위해 여기서 명시적으로 실패시킨다.
        raise RegionCodesNotFoundError(
            "법정동코드 마스터가 비어 있습니다. 법정동코드를 먼저 적재해야 합니다."
        )

    codes = []
    for code in rows:
        # 마스터 값이 그대로 API 요청 파라미터가 되므로, 보내기 전에 형식을 검증한다.
        if not LAWD_CD_PATTERN.fullmatch(code):
            raise ValueError(f"올바르지 않은 법정동 코드입니다: {code!r}")
        codes.append(code)
    return codes


def _recent_months(collection_months: int, today: date | None = None) -> list[str]:
    # 현재 월부터 collection_months개월 전까지(과거→현재 오름차순)의 YYYYMM 목록.
    # 예: 24개월이고 오늘이 2026-09이면 202410 ~ 202609.
    today = today or datetime.now(KST).date()
    year, month = today.year, today.month
    months = []
    for _ in range(collection_months):
        months.append(f"{year:04d}{month:02d}")
        month -= 1
        if month == 0:
            month = 12
            year -= 1
    months.reverse()
    return months


def _parse_item_element(item_el: ET.Element) -> dict[str, str]:
    return {child.tag: (child.text or "") for child in item_el}


def _parse_response(root: ET.Element) -> tuple[str, str, list[dict[str, str]], int]:
    result_code = root.findtext("./header/resultCode") or ""
    result_msg = root.findtext("./header/resultMsg") or ""

    items_el = root.find("./body/items")
    items = (
        []
        if items_el is None
        else [_parse_item_element(item_el) for item_el in items_el.findall("item")]
    )

    total_count = int(root.findtext("./body/totalCount") or 0)

    return result_code, result_msg, items, total_count


async def _retry_or_raise(
    attempt: int, error_message: str, cause: Exception | None = None
) -> None:
    if attempt > MAX_TRANSIENT_RETRIES:
        raise FatalApiError(error_message) from cause
    await asyncio.sleep(RETRY_BACKOFF_SECONDS * attempt)


class DataCollector(ABC):
    # 수집 작업이 어떤 수집 모듈이든 같은 방식으로 만들 수 있도록 생성자를 맞춰 둔다.
    # session_factory는 수집 대상 목록을 DB에서 읽는 모듈만, collection_months는 기간을
    # 나누어 수집하는 모듈만 실제로 사용한다.
    def __init__(
        self,
        service_key: str,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        collection_months: int = COLLECTION_MONTHS,
    ) -> None:
        self._service_key = service_key
        self._session_factory = session_factory
        self._collection_months = collection_months

    @abstractmethod
    async def collect(self) -> list[dict[str, str]]:
        """데이터를 수집하여, 응답 항목들을 그대로 반환한다."""


STAN_REGIN_CD_MAX_ROWS_PER_PAGE = 10000
STAN_REGIN_CD_SUCCESS_RESULT_CODES = {"0", "200", "300"}
STAN_REGIN_CD_DAILY_LIMIT_RESULT_CODE = "337"
STAN_REGIN_CD_TRANSIENT_RESULT_CODES = {"500", "600", "601"}


def _stan_regin_cd_result_code(root: ET.Element) -> tuple[str, str]:
    result_code = root.findtext("./head/RESULT/resultCode") or ""
    result_msg = root.findtext("./head/RESULT/resultMsg") or ""
    # 실제 응답 코드는 "INFO-0", "ERROR-337"처럼 접두어가 붙어 오므로, 접두어 뒤의
    # 코드만 떼어내 에러 코드표(숫자)와 비교한다.
    code = result_code.rsplit("-", 1)[-1]
    return code, result_msg


class StanReginCdCollector(DataCollector):
    """법정동 코드 수집 모듈"""

    BASE_URL = "https://apis.data.go.kr/1741000/StanReginCd/getStanReginCdList"

    async def collect(self) -> list[dict[str, str]]:
        all_rows: list[dict[str, str]] = []
        async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT_SECONDS) as client:
            page_no = 1
            while True:
                rows, total_count = await self._fetch_page(client, page_no)
                all_rows.extend(rows)
                if not rows or page_no * STAN_REGIN_CD_MAX_ROWS_PER_PAGE >= total_count:
                    break
                page_no += 1
        return all_rows

    async def _fetch_page(
        self, client: httpx.AsyncClient, page_no: int
    ) -> tuple[list[dict[str, str]], int]:
        params = {
            "ServiceKey": self._service_key,
            "type": "xml",
            "pageNo": page_no,
            "numOfRows": STAN_REGIN_CD_MAX_ROWS_PER_PAGE,
            "flag": "Y",
        }

        attempt = 0
        while True:
            attempt += 1
            try:
                response = await client.get(self.BASE_URL, params=params)
                response.raise_for_status()
                root = safe_xml_fromstring(response.text)
                code, result_msg = _stan_regin_cd_result_code(root)
                rows = [_parse_item_element(row_el) for row_el in root.findall("./row")]
                total_count = int(root.findtext("./head/totalCount") or 0)
            except (httpx.HTTPError, ET.ParseError) as exc:
                await _retry_or_raise(
                    attempt, f"네트워크/파싱 오류가 반복되어 중단합니다: {exc}", exc
                )
                continue

            if code in STAN_REGIN_CD_SUCCESS_RESULT_CODES:
                await asyncio.sleep(REQUEST_DELAY_SECONDS)
                return rows, total_count

            if code == STAN_REGIN_CD_DAILY_LIMIT_RESULT_CODE:
                raise DailyLimitReachedError(result_msg)

            if code in STAN_REGIN_CD_TRANSIENT_RESULT_CODES:
                await _retry_or_raise(attempt, f"API 오류 {code}: {result_msg}")
                continue

            raise FatalApiError(f"API 오류 {code}: {result_msg}")


class _RTMSCollector(DataCollector):
    """국토교통부 실거래가 API 8종은 요청/응답 형식이 동일하고 호출 URL만 다르므로,
    공통 호출/페이지네이션 로직을 여기에 두고 각 구현체는 BASE_URL만 지정한다."""

    BASE_URL: ClassVar[str]

    async def collect(self) -> list[dict[str, str]]:
        months = _recent_months(self._collection_months)
        region_codes = await _load_region_codes(self._session_factory)

        all_items: list[dict[str, str]] = []
        async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT_SECONDS) as client:
            for yyyymm in months:
                for region_code in region_codes:
                    all_items.extend(
                        await self._fetch_all_pages(client, region_code, yyyymm)
                    )
        return all_items

    async def _fetch_all_pages(
        self, client: httpx.AsyncClient, region_code: str, yyyymm: str
    ) -> list[dict[str, str]]:
        page_no = 1
        result_code, _, items, total_count = await self._fetch_page(
            client, region_code, yyyymm, page_no
        )
        if result_code == NO_DATA_RESULT_CODE:
            return []

        all_items = list(items)
        while page_no * MAX_ROWS_PER_PAGE < total_count:
            page_no += 1
            _, _, items, total_count = await self._fetch_page(
                client, region_code, yyyymm, page_no
            )
            all_items.extend(items)

        return all_items

    async def _fetch_page(
        self, client: httpx.AsyncClient, region_code: str, yyyymm: str, page_no: int
    ) -> tuple[str, str, list[dict[str, str]], int]:
        params = {
            "serviceKey": self._service_key,
            "LAWD_CD": region_code,
            "DEAL_YMD": yyyymm,
            "pageNo": page_no,
            "numOfRows": MAX_ROWS_PER_PAGE,
        }

        attempt = 0
        while True:
            attempt += 1
            try:
                response = await client.get(self.BASE_URL, params=params)
                response.raise_for_status()
                root = safe_xml_fromstring(response.text)
                result_code, result_msg, items, total_count = _parse_response(root)
            except (httpx.HTTPError, ET.ParseError) as exc:
                await _retry_or_raise(
                    attempt, f"네트워크/파싱 오류가 반복되어 중단합니다: {exc}", exc
                )
                continue

            if result_code in (SUCCESS_RESULT_CODE, NO_DATA_RESULT_CODE):
                await asyncio.sleep(REQUEST_DELAY_SECONDS)
                return result_code, result_msg, items, total_count

            if result_code == DAILY_LIMIT_RESULT_CODE:
                raise DailyLimitReachedError(result_msg)

            if result_code in TRANSIENT_RESULT_CODES:
                await _retry_or_raise(attempt, f"API 오류 {result_code}: {result_msg}")
                continue

            raise FatalApiError(f"API 오류 {result_code}: {result_msg}")


class RTMSAptTradeCollector(_RTMSCollector):
    """국토교통부 아파트 매매 실거래 데이터 수집 모듈"""

    BASE_URL = (
        "https://apis.data.go.kr/1613000/RTMSDataSvcAptTrade/getRTMSDataSvcAptTrade"
    )


class RTMSAptRentCollector(_RTMSCollector):
    """국토교통부 아파트 전월세 실거래 데이터 수집 모듈"""

    BASE_URL = (
        "https://apis.data.go.kr/1613000/RTMSDataSvcAptRent/getRTMSDataSvcAptRent"
    )


class RTMSOffiTradeCollector(_RTMSCollector):
    """국토교통부 오피스텔 매매 실거래 데이터 수집 모듈"""

    BASE_URL = (
        "https://apis.data.go.kr/1613000/RTMSDataSvcOffiTrade/getRTMSDataSvcOffiTrade"
    )


class RTMSOffiRentCollector(_RTMSCollector):
    """국토교통부 오피스텔 전월세 실거래 데이터 수집 모듈"""

    BASE_URL = (
        "https://apis.data.go.kr/1613000/RTMSDataSvcOffiRent/getRTMSDataSvcOffiRent"
    )


class RTMSRHTradeCollector(_RTMSCollector):
    """국토교통부 연립다세대 매매 실거래 데이터 수집 모듈"""

    BASE_URL = (
        "https://apis.data.go.kr/1613000/RTMSDataSvcRHTrade/getRTMSDataSvcRHTrade"
    )


class RTMSRHRentCollector(_RTMSCollector):
    """국토교통부 연립다세대 전월세 실거래 데이터 수집 모듈"""

    BASE_URL = "https://apis.data.go.kr/1613000/RTMSDataSvcRHRent/getRTMSDataSvcRHRent"


class RTMSSHTradeCollector(_RTMSCollector):
    """국토교통부 단독다가구 매매 실거래 데이터 수집 모듈"""

    BASE_URL = (
        "https://apis.data.go.kr/1613000/RTMSDataSvcSHTrade/getRTMSDataSvcSHTrade"
    )


class RTMSSHRentCollector(_RTMSCollector):
    """국토교통부 단독다가구 전월세 실거래 데이터 수집 모듈"""

    BASE_URL = "https://apis.data.go.kr/1613000/RTMSDataSvcSHRent/getRTMSDataSvcSHRent"
