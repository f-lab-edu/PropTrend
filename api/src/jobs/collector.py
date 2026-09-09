"""공공 오픈API 응답을 수집해 raw 테이블에 적재하는 수집기 인터페이스."""

import os
import xml.etree.ElementTree as ET
from abc import ABC, abstractmethod
from typing import Any

import httpx


class DataCollector(ABC):
    """API 하나의 수집 작업을 담당하는 인터페이스."""

    @abstractmethod
    async def collect(self) -> dict[str, Any]:
        """API를 호출하고 응답을 dict로 변환해 반환한다."""


class LegalDongCodeCollector(DataCollector):
    """행정안전부_행정표준코드_법정동코드(getStanReginCdList) 수집기.

    전체 법정동 약 2만건을 페이지 단위로 모두 조회하되, 그중 시군구 단위 코드만
    남긴다. 실거래가 API의 LAWD_CD는 법정동코드 10자리 중 앞 5자리(시군구)이고,
    그 하위 읍면동/리 코드로는 자료가 조회되지 않는다(해당 거래가 상위 시군구
    조회 결과에 모두 포함된다). 따라서 수집 대상으로 의미가 있는 행은 앞 5자리
    뒤가 모두 0인 'SSGGG00000' 형태의 시군구 행뿐이다.

    docs: scripts/docs/data-api/legal-dong-code.md
    """

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

        # 헤더 값은 마지막으로 받은 페이지의 것을 그대로 남긴다. totalCount는 필터링
        # 전 전체 건수이므로 len(rows)와 다르다.
        return {**page, "rows": rows}

    @staticmethod
    def _is_sigungu(row: dict[str, Any]) -> bool:
        """시군구 단위(SSGGG00000) 행인지 판별한다.

        읍면동(umd_cd)·리(ri_cd)가 모두 0이어야 하고, 시군구(sgg_cd)가 0인 시도
        단위 행(예: 1100000000 서울특별시)은 LAWD_CD로 쓸 수 없으므로 제외한다.
        """
        return (
            row.get("sgg_cd") != "000"
            and row.get("umd_cd") == "000"
            and row.get("ri_cd") == "00"
        )

    async def _fetch_page(
        self, client: httpx.AsyncClient, page_no: int
    ) -> dict[str, Any]:
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
            {field.tag: (field.text.strip() if field.text else None) for field in row}
            for row in root.findall("./row")
        ]

        return {
            "totalCount": head.findtext("totalCount") if head is not None else None,
            "numOfRows": head.findtext("numOfRows") if head is not None else None,
            "pageNo": head.findtext("pageNo") if head is not None else None,
            "type": head.findtext("type") if head is not None else None,
            "resultCode": (
                result.findtext("resultCode")
                if result is not None
                else root.findtext("./resultCode")
            ),
            "resultMsg": (
                result.findtext("resultMsg")
                if result is not None
                else root.findtext("./resultMsg")
            ),
            "rows": rows,
        }


class RtmsDataCollector(DataCollector):
    """국토교통부 실거래가 오픈API(RTMSDataSvc*) 8종의 공통 수집기.

    8개 API는 요청 파라미터(LAWD_CD/DEAL_YMD)와 응답 XML 구조가 모두 같고
    엔드포인트만 다르므로, 하위 클래스는 API_URL만 지정한다.

    수집 단위는 (지역코드 5자리, 계약년월 6자리) 하나이며, totalCount가 한
    페이지를 넘으면 남은 페이지까지 이어서 조회해 item 전체를 모아 반환한다.
    """

    API_URL: str
    MAX_ROWS_PER_PAGE = 10000
    TIMEOUT = 10
    SUCCESS_RESULT_CODE = "000"

    def __init__(self, lawd_cd: str, deal_ymd: str) -> None:
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

        # 헤더 값은 마지막으로 받은 페이지의 것을 그대로 남긴다.
        return {**page, "rows": rows}

    async def _fetch_page(
        self, client: httpx.AsyncClient, page_no: int
    ) -> dict[str, Any]:
        params = {
            "serviceKey": os.environ["DATA_GO_KR_SERVICE_KEY"],
            "LAWD_CD": self.lawd_cd,
            "DEAL_YMD": self.deal_ymd,
            "pageNo": page_no,
            "numOfRows": self.MAX_ROWS_PER_PAGE,
        }
        response = await client.get(self.API_URL, params=params)
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


class ApartSaleCollector(RtmsDataCollector):
    """국토교통부_아파트 매매 실거래가 자료(getRTMSDataSvcAptTrade) 수집기.

    docs: scripts/docs/data-api/apart-sale.md
    """

    API_URL = "https://apis.data.go.kr/1613000/RTMSDataSvcAptTrade/getRTMSDataSvcAptTrade"


class ApartRentCollector(RtmsDataCollector):
    """국토교통부_아파트 전월세 실거래가 자료(getRTMSDataSvcAptRent) 수집기.

    docs: scripts/docs/data-api/apart-rent.md
    """

    API_URL = "https://apis.data.go.kr/1613000/RTMSDataSvcAptRent/getRTMSDataSvcAptRent"


class OfficetelSaleCollector(RtmsDataCollector):
    """국토교통부_오피스텔 매매 실거래가 자료(getRTMSDataSvcOffiTrade) 수집기.

    docs: scripts/docs/data-api/officetel-sale.md
    """

    API_URL = (
        "https://apis.data.go.kr/1613000/RTMSDataSvcOffiTrade/getRTMSDataSvcOffiTrade"
    )


class OfficetelRentCollector(RtmsDataCollector):
    """국토교통부_오피스텔 전월세 실거래가 자료(getRTMSDataSvcOffiRent) 수집기.

    docs: scripts/docs/data-api/officetel-rent.md
    """

    API_URL = (
        "https://apis.data.go.kr/1613000/RTMSDataSvcOffiRent/getRTMSDataSvcOffiRent"
    )


class MultiflexSaleCollector(RtmsDataCollector):
    """국토교통부_연립다세대 매매 실거래가 자료(getRTMSDataSvcRHTrade) 수집기.

    docs: scripts/docs/data-api/multiflex-sale.md
    """

    API_URL = "https://apis.data.go.kr/1613000/RTMSDataSvcRHTrade/getRTMSDataSvcRHTrade"


class MultiflexRentCollector(RtmsDataCollector):
    """국토교통부_연립다세대 전월세 실거래가 자료(getRTMSDataSvcRHRent) 수집기.

    docs: scripts/docs/data-api/multiflex-rent.md
    """

    API_URL = "https://apis.data.go.kr/1613000/RTMSDataSvcRHRent/getRTMSDataSvcRHRent"


class SingleMultiFamilySaleCollector(RtmsDataCollector):
    """국토교통부_단독/다가구 매매 실거래가 자료(getRTMSDataSvcSHTrade) 수집기.

    docs: scripts/docs/data-api/single-multi-family-sale.md
    """

    API_URL = "https://apis.data.go.kr/1613000/RTMSDataSvcSHTrade/getRTMSDataSvcSHTrade"


class SingleMultiFamilyRentCollector(RtmsDataCollector):
    """국토교통부_단독/다가구 전월세 실거래가 자료(getRTMSDataSvcSHRent) 수집기.

    docs: scripts/docs/data-api/single-multi-family-rent.md
    """

    API_URL = "https://apis.data.go.kr/1613000/RTMSDataSvcSHRent/getRTMSDataSvcSHRent"


def _to_int(value: str | None) -> int:
    """응답의 숫자 필드를 int로 바꾼다. 값이 없거나 숫자가 아니면 0으로 본다.

    원본 API 문서/응답에 형식 오류가 잦아, 페이지 반복 조건 계산이 파싱 오류로
    중단되지 않도록 방어한다.
    """
    try:
        return int(value or 0)
    except ValueError:
        return 0
