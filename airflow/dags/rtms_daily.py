"""실거래 정기 갱신. 법정동코드와 실거래 8종의 오픈API 응답을 bronze 표에 갈아끼우고 정제 테이블로 옮긴다."""

# 이 파일은 태스크 그래프만 선언한다. 오픈API 호출·전처리·SQL·적재 루프는 rtms 패키지에 기능별로 있다.
from datetime import timedelta
from typing import Any

import pendulum
from airflow.sdk import Param, TriggerRule, dag, task
from rtms.apis import API_IDS
from rtms.tasks import (
    check_rtms,
    collect_rtms,
    plan_rtms_chunks,
    process_regions,
    process_rtms,
    refresh_legal_dong,
)

# 신고 기한 30일과 뒤늦은 해제·정정 때문에 지난 달 데이터도 계속 바뀐다. 정기 실행의 갱신 개월 수다.
RTMS_MONTHS = 2
# 수동 실행에서 늘릴 수 있는 상한. 24개월이면 API당 약 6,500회(2026-10-15 기준 시군구 269개)로 하루 호출
# 한도(10,000회) 안에 들고, 묶음은 API당 24 × 시도 16 = 384개로 Airflow의 매핑 상한(max_map_length 1024) 안에 든다.
MAX_RTMS_MONTHS = 24


@dag(
    schedule="0 4 * * *",
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=4,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=10)},
    tags=["rtms"],
    params={
        "months": Param(
            RTMS_MONTHS,
            type="integer",
            minimum=1,
            maximum=MAX_RTMS_MONTHS,
            title="갱신 개월 수",
            description="기준 계약년월부터 거슬러 올라가 갈아끼울 계약년월 수",
        ),
        # 갱신 범위의 끝 달. 비우면 회차 기준 달이라 정기 실행은 입력 없이 최근 달을 갈아끼운다.
        # 지난 달 하나가 잘못 들어갔을 때 그 달만 다시 돌리려면 끝 달을 실행 시각과 따로 정할 수 있어야 한다.
        "base_ymd": Param(
            None,
            type=["null", "string"],
            pattern=r"^\d{4}(0[1-9]|1[0-2])$",
            title="기준 계약년월",
            description="YYYYMM. 비우면 회차 기준 달이다. 예: 202503과 갱신 개월 수 1이면 2025년 3월만 갈아끼운다",
        ),
    },
)
def rtms_daily() -> None:
    """법정동코드와 지역 목록을 교체하고, API 8종을 묶음별로 bronze에 받아 정제 테이블로 옮긴다."""
    legal_dong = refresh_legal_dong()
    regions = process_regions()
    chunks = plan_rtms_chunks()

    legal_dong >> [regions, chunks]

    collects = []
    checks = []
    processes = []
    for api_id in API_IDS:
        # 묶음끼리는 단위가 겹치지 않고, 회차끼리는 max_active_runs=1로 겹치지 않는다.
        collect = collect_rtms.override(task_id=f"collect_{api_id}").partial(api_id=api_id).expand(chunk=chunks)
        check = check_rtms.override(task_id=f"check_{api_id}")(api_id)
        # 같은 API의 묶음이 다 끝난 뒤에 돈다. 필드 대조를 기다리지 않는다. 누락이 있어도 받은 bronze로 정제한다.
        process = process_rtms.override(task_id=f"process_{api_id}")(api_id)
        collect >> [check, process]
        collects.append(collect)
        checks.append(check)
        processes.append(process)

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def summarize(**context: Any) -> dict[str, dict[str, dict[str, int] | None]]:
        """API별 bronze·silver 요약을 모아 한 번에 남긴다."""
        ti = context["ti"]
        # 실패·건너뜀이면 XCom이 없다. 사유는 그리드의 태스크 상태가 말한다.
        return {
            api_id: {
                "bronze": ti.xcom_pull(task_ids=f"check_{api_id}"),
                "silver": ti.xcom_pull(task_ids=f"process_{api_id}"),
            }
            for api_id in API_IDS
        }

    @task(trigger_rule=TriggerRule.ONE_FAILED, retries=0)
    def watcher() -> None:
        """all_done 리프가 상류 실패를 가리지 않도록 DAG 실행을 실패로 만든다."""
        # DAG 실행 상태는 리프 태스크로 정해진다. summarize가 all_done으로 성공하면 상류 실패가 묻힌다.
        raise RuntimeError("갱신 태스크 중 실패한 것이 있다")

    # bronze 요약을 check에서 꺼내므로 정제와 나란히 도는 check도 기다린다.
    [*checks, *processes] >> summarize()
    [legal_dong, regions, chunks, *collects, *checks, *processes] >> watcher()


rtms_daily()
