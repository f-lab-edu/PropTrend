"""오픈API와 raw 테이블에서 데이터를 읽어오는 수집기."""

import os
from collections.abc import Sequence
from typing import Any

import httpx
from defusedxml.ElementTree import fromstring as safe_xml_fromstring
from sqlalchemy import RowMapping, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from ..model import Base
from .utils import parse_deal_ymd

# 실거래가 오픈API 8종이 공유하는 결과코드(scripts/docs/data-api/*.md의 에러 코드표).
SUCCESS_RESULT_CODE = "000"
NO_DATA_RESULT_CODE = "03"
DAILY_LIMIT_RESULT_CODE = "22"


class OpenApiError(RuntimeError):
    """오픈API가 오류 결과코드를 돌려줬다."""


class OpenApiStatusError(OpenApiError):
    """오픈API가 HTTP 오류를 돌려줬다."""

    def __init__(self, status_code: int) -> None:
        super().__init__(f"오픈API HTTP 오류: status={status_code}")
        self.status_code = status_code


class DailyLimitReachedError(OpenApiError):
    """일일 활용건수를 초과했다(결과코드 22). 남은 요청도 모두 같은 응답을 받는다."""


def _raise_for_status(response: httpx.Response) -> None:
    """HTTPStatusError 메시지에는 인증키가 실린 요청 URL이 들어 있어 상태 코드만 남긴다."""
    try:
        response.raise_for_status()
    except httpx.HTTPStatusError:
        # from None으로 원인을 끊지 않으면 logger.exception이 __cause__까지 찍어 URL이 다시 샌다.
        raise OpenApiStatusError(response.status_code) from None


def _check_rtms_result_code(result_code: str | None, result_msg: str | None) -> None:
    """정상(000)과 데이터없음(03) 외에는 수집을 진행시키지 않는다.

    일시적 오류(01·02·04·05)도 예외로 올린다. 백필용 collect_rtms.py는 재시도하지만,
    이쪽은 매일 도는 갱신이라 실패한 단위를 다음 회차가 다시 가져간다. 비동기 백오프는
    기다리는 동안 세마포어 슬롯을 쥐고 있어 배치 전체를 늦춘다.
    """
    if result_code == DAILY_LIMIT_RESULT_CODE:
        raise DailyLimitReachedError(f"오픈API 일일 호출 제한: {result_msg}")
    if result_code not in (SUCCESS_RESULT_CODE, NO_DATA_RESULT_CODE):
        raise OpenApiError(f"오픈API 오류 {result_code}: {result_msg}")


class LegalDongCodeCollector:
    """행정안전부_행정표준코드_법정동코드(getStanReginCdList) 수집기."""

    API_URL = "https://apis.data.go.kr/1741000/StanReginCd/getStanReginCdList"
    MAX_ROWS_PER_PAGE = 1000  # 1회 요청 최대 건수(초과 시 에러코드 336)
    TIMEOUT = 10
    SUCCESS_RESULT_CODE_PREFIX = "INFO"

    async def collect(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        page_no = 1

        async with httpx.AsyncClient(timeout=self.TIMEOUT) as client:
            while True:
                page = await self._fetch_page(client, page_no)
                rows.extend(row for row in page["rows"] if self._is_sigungu(row))

                if page_no * self.MAX_ROWS_PER_PAGE >= _to_int(page["totalCount"]):
                    break
                page_no += 1

        if not rows:
            # 호출부가 이 목록으로 법정동코드 표를 통째로 갈아끼운다. 데이터없음(INFO-200)도
            # 접두사 검사를 통과하므로, 빈 목록을 정상으로 넘기면 표가 비고 이후 모든 갱신이
            # 시군구 목록을 잃는다.
            raise OpenApiError("법정동코드 응답에 시군구 행이 없다")

        return rows

    @staticmethod
    def _is_sigungu(row: dict[str, Any]) -> bool:
        """시군구 단위(SSGGG00000) 행인지 판별한다."""
        return row.get("sgg_cd") != "000" and row.get("umd_cd") == "000" and row.get("ri_cd") == "00"

    async def _fetch_page(self, client: httpx.AsyncClient, page_no: int) -> dict[str, Any]:
        params = {
            "ServiceKey": os.environ["DATA_GO_KR_SERVICE_KEY"],
            "type": "xml",
            "pageNo": page_no,
            "numOfRows": self.MAX_ROWS_PER_PAGE,
            "flag": "Y",
        }
        response = await client.get(self.API_URL, params=params)
        _raise_for_status(response)

        root = safe_xml_fromstring(response.text)

        head = root.find("./head")
        result = head.find("./RESULT") if head is not None else None
        # 에러 응답은 head 없이 최상위에 resultCode/resultMsg만 담겨 온다.
        result_code = (result.findtext("resultCode") if result is not None else root.findtext("./resultCode")) or ""
        result_msg = result.findtext("resultMsg") if result is not None else root.findtext("./resultMsg")
        if not result_code.startswith(self.SUCCESS_RESULT_CODE_PREFIX):
            raise OpenApiError(f"법정동코드 API 오류 {result_code}: {result_msg}")

        rows = [
            {field.tag: (field.text.strip() if field.text else None) for field in row} for row in root.findall("./row")
        ]

        return {
            "totalCount": head.findtext("totalCount") if head is not None else None,
            "rows": rows,
        }


class RtmsDataCollector:
    """국토교통부 실거래가 오픈API(RTMSDataSvc*) 8종의 공통 수집기. URL만 갈아 끼운다."""

    MAX_ROWS_PER_PAGE = 10000
    TIMEOUT = 10

    def __init__(self, api_url: str, lawd_cd: str, deal_ymd: str) -> None:
        self.api_url = api_url
        self.lawd_cd = lawd_cd
        self.deal_ymd = deal_ymd

    async def collect(self) -> list[dict[str, Any]]:
        async with httpx.AsyncClient(timeout=self.TIMEOUT) as client:
            page = await self._fetch_page(client, 1)
            # 해당 시군구·계약년월에 거래가 없는 달은 정상이다. 빈 목록으로 갱신하면 그만이다.
            if page["resultCode"] == NO_DATA_RESULT_CODE:
                return []

            rows = list(page["rows"])
            page_no = 1
            while page_no * self.MAX_ROWS_PER_PAGE < _to_int(page["totalCount"]):
                page_no += 1
                page = await self._fetch_page(client, page_no)
                rows.extend(page["rows"])

        return rows

    async def _fetch_page(self, client: httpx.AsyncClient, page_no: int) -> dict[str, Any]:
        params = {
            "serviceKey": os.environ["DATA_GO_KR_SERVICE_KEY"],
            "LAWD_CD": self.lawd_cd,
            "DEAL_YMD": self.deal_ymd,
            "pageNo": page_no,
            "numOfRows": self.MAX_ROWS_PER_PAGE,
        }
        response = await client.get(self.api_url, params=params)
        _raise_for_status(response)

        root = safe_xml_fromstring(response.text)

        result_code = root.findtext("./header/resultCode")
        _check_rtms_result_code(result_code, root.findtext("./header/resultMsg"))

        rows = [
            {field.tag: (field.text.strip() if field.text else None) for field in item}
            for item in root.findall("./body/items/item")
        ]

        return {
            "totalCount": root.findtext("./body/totalCount"),
            "resultCode": result_code,
            "rows": rows,
        }


class RawTableCollector:
    """raw 테이블 하나에서 갱신 단위만큼 원본 행을 읽어오는 수집기."""

    def __init__(self, session: AsyncSession, model: type[Base]) -> None:
        self.session = session
        self.model = model

    async def collect(self, deal_ymd: str, sgg_cd: str | None = None) -> Sequence[RowMapping]:
        """(계약년월, 시군구) 단위의 원본 행을 모두 읽어 반환한다."""
        year, month = parse_deal_ymd(deal_ymd)

        table = self.model.__table__
        conditions = [
            table.c.dealYear == year,
            # 원본 dealMonth는 "07"이 아니라 "7"로 들어온다.
            func.lpad(table.c.dealMonth, 2, "0") == month,
        ]
        if sgg_cd is not None:
            conditions.append(table.c.sggCd == sgg_cd)

        result = await self.session.execute(select(table).where(*conditions))
        return result.mappings().all()


def _to_int(value: str | None) -> int:
    """응답의 숫자 필드를 int로 바꾼다. 값이 없거나 숫자가 아니면 0으로 본다."""
    try:
        return int(value or 0)
    except ValueError:
        return 0
