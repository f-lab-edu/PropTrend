# scripts

국토교통부/행정안전부 공공데이터를 수집하고, 수집한 원본을 DB에 적재하는 스크립트 모음.

## 수집

`scripts/results` 아래에 원본 응답을 그대로 남긴다. `.env`에 `DATA_GO_KR_SERVICE_KEY`가 필요하다.

```bash
uv run --project scripts python scripts/src/collect_legal_dong_code.py
uv run --project scripts python scripts/src/collect_rtms.py
```

`collect_legal_dong_code.py`는 한 번 받아 두 벌로 남긴다. 실거래가 수집은 시군구 목록이
있어야 시작하므로 이 스크립트를 먼저 돌린다.

| 파일 | 내용 | 읽는 곳 |
|---|---|---|
| `legal_dong_code_raw.json` | 응답 전 행(읍면동·리 포함, 13필드 전체) | `load_results.py` → bronze 표 |
| `legal_dong_code.json` | 시군구만 `{code, name}`으로 줄인 목록 | `collect_rtms.py`의 `LAWD_CD` |

## 적재 (`load_results.py`)

`scripts/results`에 쌓인 월별 원본 JSON을 **가공 없이 그대로** bronze 표 `rtms_raw_items`에
적재하는 **일회성 배치**다. 응답 item 하나가 JSONB `payload` 한 칸에 통째로 들어가 변환 계층이
없고, 실거래가 8종이 `api_id`만 바꿔가며 같은 표를 쓴다.

표 정의와 적재 로직은 이 스크립트 안에 직접 들어 있다. `api` 프로젝트의 모델이나 적재기를
import하지 않으므로, 서비스 코드가 리팩터링돼도 백필이 따라 깨지지 않는다. 대신 DDL이 두 벌이라
`api/src/model/raw.py`와 `api/src/model/load_progress.py`가 바뀌면 스크립트도 같이 고쳐야 한다.
`sqlalchemy`와 `asyncpg`가 `api` 쪽 의존성이라 **실행만 api 가상환경으로** 한다.

```bash
docker compose up -d postgres

# 시범 적재
uv run --project api python scripts/src/load_results.py --api officetel_sale --from 202501 --to 202506

# 전체 적재 (약 4,200만 행, 25분 내외, 백그라운드 권장)
uv run --project api python scripts/src/load_results.py 2>&1 | tee /tmp/load_results.log
```

테이블이 없으면 먼저 만들고(`MetaData.create_all`), 8종 실거래가를 월 오름차순으로 넣는다.
접속 정보는 `api/.env`의 `DATABASE_URL`을 읽으며, 없으면 `docker-compose.yml`과 같은 기본값을 쓴다.

| 옵션 | 설명 |
|---|---|
| `--api <id>` | 특정 API만 적재. 여러 번 지정 가능, 생략 시 8종 + `legal_dong_code` 전부 |
| `--from YYYYMM` / `--to YYYYMM` | 계약년월 범위 제한 |
| `--dry-run` | 파일을 읽어 행수만 세고 적재는 생략. 진행 기록도 남기지 않는다 |
| `--restart` | 이번 실행에서만 진행 기록을 무시하고 전부 다시 적재. 기록은 지우지 않는다 |
| `--truncate` | 대상 `api_id`의 bronze 행과 진행 기록을 지운 뒤 처음부터. `--from`/`--to` 범위 한정 |
| `--no-create-tables` | 테이블 생성 생략. `raw_load_progress`도 안 만들어지니 주의 |

### `lawd_cd`는 `sggCd`에서 되찾는다

`rtms_raw_items.lawd_cd`는 원래 요청 파라미터(`LAWD_CD`)에서 오는 값이다. 그런데 수집기가
한 달치 지역을 한 파일로 합치면서(`finalize_month`) 그 파라미터를 버려, 백필에 남은 출처는
`payload`의 `sggCd`뿐이다. 스크립트는 거기서 값을 되찾고 5자리인지 검증한다.

값이 없거나 형식이 틀리면 **그 파일을 통째로 실패시킨다.** 임의의 값으로 채우면 갱신
파이프라인의 단위 삭제가 그 행을 못 찾아 매일 중복이 쌓이고, 조용히 버리면 같은 달을 다시
받을 때 빠진 만큼이 아니라 전부가 다시 들어온다.

### 재실행과 멱등성

bronze 표에는 `id` PK 외에 unique 제약이 없어 **같은 파일을 두 번 넣으면 행이 그대로 중복된다.**
그래서 진행 기록이 편의가 아니라 정합성을 책임진다.

- 진행 기록은 `raw_load_progress` 표의 `(api_id, yyyymm)` 행이다. 원본 파일 하나가 한 행이다.
- **적재와 완료 기록이 한 트랜잭션이다.** 둘 다 들어가거나 둘 다 롤백된다. 중간에 죽어도
  절반만 적재된 파일이 남지 않고, 반대로 "적재는 됐는데 기록이 없어 다시 넣는" 창도 없다.
- 다시 실행하면 기록에 없는 파일부터 이어간다. 실패한 파일도 기록에 없으니 그것만 재시도된다.
- 처음부터 깨끗이 다시 넣으려면 `--restart`가 아니라 **`--truncate`**를 쓴다. `--restart`는
  기록을 무시할 뿐 기존 행을 지우지 않아 중복이 생긴다(실행 시 경고가 뜬다).

`--truncate`는 `TRUNCATE`가 아니라 **범위를 좁힌 `DELETE`**다. 8종이 `rtms_raw_items` 하나를
나눠 쓰므로 표를 통째로 비우면 고르지 않은 API까지 날아간다. `--api`로 고른 `api_id`와
`--from`/`--to` 월 범위에만 적용되며, 지운 뒤 곧바로 그 구간을 다시 적재한다.

### 갱신 파이프라인이 이미 채운 달

일일 갱신 파이프라인도 같은 표에 쓴다. 그래서 진행 기록만으로는 중복을 못 막는다. 그 행을 넣은
건 이 스크립트가 아니기 때문이다. 적재를 시작하기 전에 이번에 넣을 `(api_id, deal_ymd)` 중
**bronze에 이미 행이 있는데 진행 기록에는 없는** 단위를 찾아, 있으면 종료 코드 2로 멈춘다.

```
ERROR 진행 기록에 없는데 bronze에 이미 행이 있는 단위가 1개 있습니다:
ERROR   apart_sale/202608
ERROR 그대로 적재하면 같은 달을 두 번 넣어 행이 통째로 중복됩니다.
```

어느 쪽이 더 완전한지 보고 고른다. 파이프라인이 일부 구만 긁다 만 달이면 `--truncate`로 그
달만 지우고 파일로 덮고, 파일보다 파이프라인 쪽이 최신이면 `--from`/`--to`로 그 달을 빼고 넣는다.
중복을 감수하고 그냥 넣으려면 `--restart`를 쓴다.

### 법정동코드는 따로 다룬다

`legal_dong_code`는 월 단위가 없어 `(api_id, yyyymm)` 진행 기록에 얹히지 않는다. 대신
`legal_dong_code_raw_items`를 **통째로 비우고 다시 채운다.** 몇 번을 돌려도 결과가 같아
이어갈 지점이 필요 없고, 비우기와 채우기는 한 트랜잭션이다. `api/src/jobs/pipeline.py`의
`refresh_legal_dong_codes`와 같은 방식이며, 실거래가보다 먼저 적재한다.

원본 파일에는 읍면동·리까지 20,000행 넘게 들어 있지만 **표에는 시군구 행만 넣는다.**
갱신 파이프라인이 그렇게 채우고 `sigungu_codes()`가 이 표를 `LAWD_CD` 목록으로 읽으므로,
하위 행까지 넣으면 다음 수집이 존재하지 않는 지역을 긁는다. 판별 규칙은
`api/src/jobs/collector.py`의 `_is_sigungu`와 같다.

원본 파일이 없으면 경고만 남기고 건너뛴다. 시군구 행이 하나도 없으면 표를 비우지 않고
실패시킨다. 빈 목록으로 갈아끼우면 이후 모든 수집이 시군구 목록을 잃기 때문이다.
