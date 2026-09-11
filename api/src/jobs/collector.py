"""오픈API와 raw 테이블에서 데이터를 읽어오는 수집기."""

import os
import xml.etree.ElementTree as ET
from collections.abc import Sequence
from typing import Any

import httpx
from sqlalchemy import RowMapping, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from ..model import Base
from .utils import parse_deal_ymd


class LegalDongCodeCollector:
    """행정안전부_행정표준코드_법정동코드(getStanReginCdList) 수집기."""

    API_URL = "https://apis.data.go.kr/1741000/StanReginCd/getStanReginCdList"
    MAX_ROWS_PER_PAGE = 1000  # 1회 요청 최대 건수(초과 시 에러코드 336)
    TIMEOUT = 10
    SUCCESS_RESULT_CODE_PREFIX = "INFO"

    async def collect(self) -> dict[str, Any]:
        rows: list[dict[str, Any]] = []
        page_no = 1

        async with httpx.AsyncClient(timeout=self.TIMEOUT) as client:
            while True:
                page = await self._fetch_page(client, page_no)
                rows.extend(row for row in page["rows"] if self._is_sigungu(row))

                result_code = page["resultCode"] or ""
                if not result_code.startswith(self.SUCCESS_RESULT_CODE_PREFIX):
                    break
                if page_no * self.MAX_ROWS_PER_PAGE >= _to_int(page["totalCount"]):
                    break
                page_no += 1

        return {**page, "rows": rows}

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
        response.raise_for_status()

        root = ET.fromstring(response.text)

        head = root.find("./head")
        result = head.find("./RESULT") if head is not None else None
        # 에러 응답은 head 없이 최상위에 resultCode/resultMsg만 담겨 온다.
        rows = [
            {field.tag: (field.text.strip() if field.text else None) for field in row} for row in root.findall("./row")
        ]

        return {
            "totalCount": head.findtext("totalCount") if head is not None else None,
            "numOfRows": head.findtext("numOfRows") if head is not None else None,
            "pageNo": head.findtext("pageNo") if head is not None else None,
            "type": head.findtext("type") if head is not None else None,
            "resultCode": (result.findtext("resultCode") if result is not None else root.findtext("./resultCode")),
            "resultMsg": (result.findtext("resultMsg") if result is not None else root.findtext("./resultMsg")),
            "rows": rows,
        }


class RtmsDataCollector:
    """국토교통부 실거래가 오픈API(RTMSDataSvc*) 8종의 공통 수집기. URL만 갈아 끼운다."""

    MAX_ROWS_PER_PAGE = 10000
    TIMEOUT = 10
    SUCCESS_RESULT_CODE = "000"

    def __init__(self, api_url: str, lawd_cd: str, deal_ymd: str) -> None:
        self.api_url = api_url
        self.lawd_cd = lawd_cd
        self.deal_ymd = deal_ymd

    async def collect(self) -> dict[str, Any]:
        rows: list[dict[str, Any]] = []
        page_no = 1

        async with httpx.AsyncClient(timeout=self.TIMEOUT) as client:
            while True:
                page = await self._fetch_page(client, page_no)
                rows.extend(page["rows"])

                # 데이터없음(03)을 포함해 정상(000)이 아니면 더 넘길 페이지가 없다.
                if page["resultCode"] != self.SUCCESS_RESULT_CODE:
                    break
                if page_no * self.MAX_ROWS_PER_PAGE >= _to_int(page["totalCount"]):
                    break
                page_no += 1

        return {**page, "rows": rows}

    async def _fetch_page(self, client: httpx.AsyncClient, page_no: int) -> dict[str, Any]:
        params = {
            "serviceKey": os.environ["DATA_GO_KR_SERVICE_KEY"],
            "LAWD_CD": self.lawd_cd,
            "DEAL_YMD": self.deal_ymd,
            "pageNo": page_no,
            "numOfRows": self.MAX_ROWS_PER_PAGE,
        }
        response = await client.get(self.api_url, params=params)
        response.raise_for_status()

        root = ET.fromstring(response.text)

        rows = [
            {field.tag: (field.text.strip() if field.text else None) for field in item}
            for item in root.findall("./body/items/item")
        ]

        return {
            "totalCount": root.findtext("./body/totalCount"),
            "numOfRows": root.findtext("./body/numOfRows"),
            "pageNo": root.findtext("./body/pageNo"),
            "resultCode": root.findtext("./header/resultCode"),
            "resultMsg": root.findtext("./header/resultMsg"),
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
