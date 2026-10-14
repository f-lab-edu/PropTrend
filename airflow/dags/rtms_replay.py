"""실거래 지정 재실행. 사람이 찍은 갱신 단위만 정제 테이블을, with_collect면 bronze부터 다시 갈아끼운다."""

from contextlib import closing
from datetime import timedelta
from typing import Any, cast

import pendulum
from airflow.providers.postgres.hooks.postgres import PostgresHook

# [Airflow] Param은 수동 실행 때 받을 입력값이다. JSON Schema로 검사해 형식이 틀리면 실행 자체를 만들지 않는다.
from airflow.sdk import Param, TriggerRule, dag, task
from airflow.sdk.exceptions import AirflowSkipException
from rtms.open_api import API_IDS
from rtms.refresh import PROP_TREND_DB_CONN_ID, collect_units, process_units, sigungu_codes, try_lock

# api_id:lawd_cd:deal_ymd. [0-9]로 적어야 비ASCII 숫자가 통과하지 않는다.
UNIT_PATTERN = rf"^({'|'.join(API_IDS)}):[0-9]{{5}}:[0-9]{{4}}(0[1-9]|1[0-2])$"


def replay_units(hook: PostgresHook, api_id: str, coordinates: list[str]) -> list[tuple[str, str]]:
    """입력 좌표 중 이 API의 단위를 중복 없이 고른다. 없으면 건너뛰고, 모르는 시군구가 있으면 실패한다."""
    units = [
        (lawd_cd, deal_ymd)
        for coordinate in dict.fromkeys(coordinates)
        for coordinate_api_id, lawd_cd, deal_ymd in [coordinate.split(":")]
        if coordinate_api_id == api_id
    ]
    if not units:
        raise AirflowSkipException(f"{api_id} 단위를 지정하지 않았다")
    # 형식은 Param이 걸렀지만 시군구가 실제로 있는지는 DB를 봐야 안다. 오타 난 시군구는 수집하면 0건으로,
    # 정제만 하면 bronze 없음으로 끝나 "왜 안 바뀌지"가 된다. 아무것도 건드리기 전에 멈춘다.
    if unknown := sorted({lawd_cd for lawd_cd, _ in units} - set(sigungu_codes(hook))):
        raise ValueError(f"법정동코드에 없는 시군구다: {unknown}")
    return units


def lock_or_fail(conn: Any, api_id: str) -> None:
    """범위 락을 잡는다. 다른 실행이 쥐고 있으면 태스크를 실패시킨다."""
    try:
        try_lock(conn, api_id)
    except AirflowSkipException as error:
        # 사람이 결과를 기다리는 실행이다. 건너뜀으로 끝나면 DAG 실행이 성공으로 보여 안 돈 것을 놓친다.
        raise RuntimeError(f"{error}. 그 실행이 끝난 뒤 다시 돌려야 한다") from error


@task(execution_timeout=timedelta(hours=1))
def collect_replay(api_id: str, **context: Any) -> dict[str, int]:
    """지정한 단위의 오픈API 응답을 받아 bronze를 갈아끼운다."""
    # [Airflow] context["params"]는 DAG의 Param 기본값에 실행 때 넣은 값을 덮은 것이다.
    params = context["params"]
    if not params["with_collect"]:
        raise AirflowSkipException("with_collect가 꺼져 있어 bronze에 남은 응답으로 정제만 한다")
    hook = PostgresHook(postgres_conn_id=PROP_TREND_DB_CONN_ID)
    units = replay_units(hook, api_id, params["units"])
    with closing(cast("Any", hook.get_conn())) as conn:
        lock_or_fail(conn, api_id)
        return collect_units(conn, api_id, units)


# 수집이 꺼져 건너뛰었거나 실패해도 bronze에 남은 응답으로 정제한다.
@task(trigger_rule=TriggerRule.ALL_DONE, execution_timeout=timedelta(minutes=30))
def process_replay(api_id: str, **context: Any) -> dict[str, int]:
    """지정한 단위의 정제 테이블을 bronze에 남은 응답으로 갈아끼운다."""
    params = context["params"]
    hook = PostgresHook(postgres_conn_id=PROP_TREND_DB_CONN_ID)
    units = replay_units(hook, api_id, params["units"])
    with closing(cast("Any", hook.get_conn())) as conn:
        lock_or_fail(conn, api_id)
        # 정제만 돌릴 때는 bronze가 빈 단위를 지우지 않는다. 수집부터 돌렸다면 빈 응답이 곧 거래 없는 단위다.
        return process_units(conn, api_id, units, require_bronze=not params["with_collect"])


@dag(
    schedule=None,
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Seoul"),
    catchup=False,
    # 같은 단위를 두 실행이 동시에 갈아끼우지 않게 한다. 겹치면 뒤 실행이 락에 막혀 실패한다.
    max_active_runs=1,
    max_active_tasks=4,
    # 사람이 결과를 보고 다시 실행한다. 자동 재시도는 같은 실패를 한 번 더 기다리게 할 뿐이다.
    default_args={"retries": 0},
    params={
        "units": Param(
            type="array",
            items={"type": "string", "pattern": UNIT_PATTERN},
            minItems=1,
            title="갱신 단위",
            description="api_id:lawd_cd:deal_ymd를 한 줄에 하나씩 적는다 (예: apart_sale:11110:202602)",
        ),
        "with_collect": Param(
            False,
            type="boolean",
            title="수집부터",
            description="오픈API에서 다시 받은 뒤 정제한다. 끄면 bronze에 남은 응답으로 정제만 한다",
        ),
    },
    tags=["rtms"],
)
def rtms_replay() -> None:
    """입력한 단위를 API별로 나눠 수집(선택)과 정제를 다시 돌린다."""
    tasks = []
    for api_id in API_IDS:
        # 정기 갱신과 같은 pool에 줄 서서 같은 API의 한도를 동시에 쓰지 않는다.
        collect = collect_replay.override(task_id=f"collect_{api_id}", pool=f"rtms_{api_id}")(api_id)
        process = process_replay.override(task_id=f"process_{api_id}")(api_id)
        collect >> process
        tasks += [collect, process]

    @task(trigger_rule=TriggerRule.ONE_FAILED, retries=0)
    def watcher() -> None:
        """정제가 all_done으로 성공해 수집 실패를 가리지 않도록 DAG 실행을 실패로 만든다."""
        raise RuntimeError("재실행 태스크 중 실패한 것이 있다")

    tasks >> watcher()


rtms_replay()
