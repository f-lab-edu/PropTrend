import asyncio
import json
import logging
from collections.abc import Iterator

import boto3
from botocore.client import BaseClient

from app.config import get_settings
from app.db import async_session_factory
from app.model import PropertyType
from app.scheduler.collect import (
    API_CONFIGS,
    REGION_CODE_KEY,
    _load_region_codes,
    _recent_yyyymm_range,
    collect_one_api,
)
from app.scheduler.ingest import ingest_file
from app.scheduler.legal_dong import collect_sigungu_codes

logger = logging.getLogger(__name__)

settings = get_settings()
S3_BUCKET = settings.s3_bucket
S3_EXPECTED_BUCKET_OWNER = settings.s3_expected_bucket_owner
S3_PREFIX = settings.s3_prefix

# api_id -> (property_type, "sale" | "rent")
SOURCE_DIRS: dict[str, tuple[PropertyType, str]] = {
    "apart_sale": (PropertyType.APT, "sale"),
    "apart_rent": (PropertyType.APT, "rent"),
    "officetel_sale": (PropertyType.OFFICETEL, "sale"),
    "officetel_rent": (PropertyType.OFFICETEL, "rent"),
    "multiflex_sale": (PropertyType.ROW_HOUSE, "sale"),
    "multiflex_rent": (PropertyType.ROW_HOUSE, "rent"),
    "single_multi_family_sale": (PropertyType.SINGLE_MULTI, "sale"),
    "single_multi_family_rent": (PropertyType.SINGLE_MULTI, "rent"),
}


def _list_json_keys(s3_client: BaseClient, prefix: str) -> Iterator[str]:
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(
        Bucket=S3_BUCKET, Prefix=prefix, ExpectedBucketOwner=S3_EXPECTED_BUCKET_OWNER
    ):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".json"):
                yield key


def _read_object(s3_client: BaseClient, key: str) -> str:
    response = s3_client.get_object(
        Bucket=S3_BUCKET, Key=key, ExpectedBucketOwner=S3_EXPECTED_BUCKET_OWNER
    )
    return response["Body"].read().decode("utf-8")


async def run_legal_dong_code_job() -> None:
    """행정표준코드관리시스템에서 시군구 법정동 코드 전체를 조회해 S3에 업로드한다.

    run_data_collection_job이 지역 코드(LAWD_CD) 목록을 읽어오는 파일을 준비하는
    작업이므로, 파이프라인에서 가장 먼저 실행되어야 한다."""
    s3_client = boto3.client("s3")
    region_codes = await asyncio.to_thread(collect_sigungu_codes)
    payload = json.dumps(region_codes, ensure_ascii=False, indent=2).encode("utf-8")
    await asyncio.to_thread(
        s3_client.put_object,
        Bucket=S3_BUCKET,
        Key=REGION_CODE_KEY,
        Body=payload,
        ContentType="application/json",
        ExpectedBucketOwner=S3_EXPECTED_BUCKET_OWNER,
    )
    logger.info(
        "legal dong code job finished: %d regions uploaded to %s",
        len(region_codes),
        REGION_CODE_KEY,
    )


async def run_data_collection_job() -> None:
    """RTMS 8개 API에서 오늘 기준 최근 2개년치 실거래가를 조회해 S3에 업로드한다.

    run_data_extraction_job이 읽어갈 파일을 준비/갱신하는 선행 작업이며, 체크포인트
    없이 매 실행마다 전체 구간(최근 24개월 x 전 지역)을 다시 수집해 동일 키를
    덮어쓴다."""
    s3_client = boto3.client("s3")
    region_codes = await asyncio.to_thread(_load_region_codes, s3_client)
    months = _recent_yyyymm_range()

    async def _run(api_id: str, base_url: str) -> tuple[str, int]:
        count = await asyncio.to_thread(
            collect_one_api, s3_client, api_id, base_url, region_codes, months
        )
        return api_id, count

    results = await asyncio.gather(
        *(_run(api_id, base_url) for api_id, base_url in API_CONFIGS)
    )
    for api_id, count in results:
        logger.info(
            "data collection job: [%s] %d/%d months uploaded",
            api_id,
            count,
            len(months),
        )


async def run_data_extraction_job() -> None:
    """S3의 api_id별 prefix 아래 월별 실거래가 JSON을 읽어 DB에 배치 적재한다."""
    s3_client = boto3.client("s3")
    total = 0
    async with async_session_factory() as session:
        for api_id, (property_type, kind) in SOURCE_DIRS.items():
            prefix = f"{S3_PREFIX}{api_id}/"
            keys = await asyncio.to_thread(
                lambda p=prefix: list(_list_json_keys(s3_client, p))
            )
            if not keys:
                logger.warning("no objects found under prefix: %s", prefix)
                continue
            for key in sorted(keys):
                try:
                    content = await asyncio.to_thread(_read_object, s3_client, key)
                    total += await ingest_file(session, content, property_type, kind)
                    await session.commit()
                except Exception:
                    await session.rollback()
                    logger.exception("failed to ingest object: %s", key)
    logger.info("data extraction job finished: %d rows processed", total)


async def run_scheduled_pipeline() -> None:
    """법정동 코드 갱신 -> 수집(외부 API -> S3) -> 적재(S3 -> DB) 순서로 실행한다."""
    await run_legal_dong_code_job()
    await run_data_collection_job()
    await run_data_extraction_job()
