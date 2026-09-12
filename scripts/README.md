# scripts

국토교통부/행정안전부 공공데이터를 수집하고, 수집한 원본을 DB에 적재하는 스크립트 모음.

## 수집

`scripts/results` 아래에 원본 응답을 그대로 남긴다. `.env`에 `DATA_GO_KR_SERVICE_KEY`가 필요하다.

```bash
uv run --project scripts python scripts/src/collect_legal_dong_code.py
uv run --project scripts python scripts/src/collect_rtms.py
```

## 적재 (`load_results.py`)

`scripts/results`에 쌓인 월별 원본 JSON을 **가공 없이 그대로** raw 테이블 8종에 적재하는
**일회성 배치**다. raw 테이블은 API 응답 필드명을 그대로 컬럼명으로 쓰므로 변환 계층이 없다.
적재 로직(`RawDataLoader`)과 모델이 `api` 프로젝트에 있으므로 **api 가상환경으로 실행**한다.

```bash
docker compose up -d postgres

# 시범 적재
uv run --project api python scripts/src/load_results.py --api officetel_sale --from 202501 --to 202506

# 전체 적재 (약 4,200만 행, 20분 내외, 백그라운드 권장)
uv run --project api python scripts/src/load_results.py 2>&1 | tee /tmp/load_results.log
```

테이블이 없으면 먼저 만들고(`Base.metadata.create_all`), 8종 실거래가를 월 오름차순으로 넣는다.
접속 정보는 `api/.env`의 `DATABASE_URL`을 읽으며, 없으면 `docker-compose.yml`과 같은 기본값을 쓴다.

| 옵션 | 설명 |
|---|---|
| `--api <id>` | 특정 API만 적재. 여러 번 지정 가능, 생략 시 8종 전부 |
| `--from YYYYMM` / `--to YYYYMM` | 계약년월 범위 제한 |
| `--dry-run` | 파일을 읽어 행수만 세고 적재는 생략. 진행 기록도 남기지 않는다 |
| `--restart` | 이번 실행에서만 진행 기록을 무시하고 전부 다시 적재. 기록은 지우지 않는다 |
| `--truncate` | 적재 대상 테이블과 **해당 API의** 진행 기록을 비운 뒤 처음부터 |
| `--no-create-tables` | 테이블 생성 생략. `raw_load_progress`도 안 만들어지니 주의 |
| `--import-legacy-progress` | 파일로 남은 옛 진행 기록을 표로 옮기고 적재 없이 끝낸다 |

### 재실행과 멱등성

raw 테이블에는 `id` PK 외에 unique 제약이 없어 **같은 파일을 두 번 넣으면 행이 그대로 중복된다.**
그래서 진행 기록이 편의가 아니라 정합성을 책임진다.

- 진행 기록은 `raw_load_progress` 표의 `(api_id, yyyymm)` 행이다. 원본 파일 하나가 한 행이다.
- **적재와 완료 기록이 한 트랜잭션이다.** 둘 다 들어가거나 둘 다 롤백된다. 중간에 죽어도
  절반만 적재된 파일이 남지 않고, 반대로 "적재는 됐는데 기록이 없어 다시 넣는" 창도 없다.
- 다시 실행하면 기록에 없는 파일부터 이어간다. 실패한 파일도 기록에 없으니 그것만 재시도된다.
- 기록이 API별 행이므로 `--truncate --api apart_sale`은 `apart_sale`의 기록만 지운다.
  나머지 API는 그대로 건너뛴다.
- 처음부터 깨끗이 다시 넣으려면 `--restart`가 아니라 **`--truncate`**를 쓴다. `--restart`는
  기록을 무시할 뿐 기존 행을 지우지 않아 중복이 생긴다(실행 시 경고가 뜬다).

### 옛 진행 기록 옮기기 (한 번만)

기록이 `scripts/results/load_raw.progress.json` 파일에 있던 시절의 기록이 남아 있다면,
표로 옮기기 전에는 적재를 시작할 수 없다. 그냥 돌리면 이미 들어간 파일을 다시 넣는다.

```bash
uv run --project api python scripts/src/load_results.py --import-legacy-progress
```

옛 파일이 남아 있는데 `raw_load_progress`가 비어 있으면 스크립트가 종료 코드 2로 멈추고
무엇을 해야 하는지 안내한다. 자동으로 옮기지 않는 이유는, 그 파일이 지금 `DATABASE_URL`이
가리키는 DB의 기록이라는 보장이 없기 때문이다. 빈 DB에 처음부터 넣는 것이라면 `--truncate`를 쓴다.

### 범위에서 빠진 것

`raw_legal_dong_code`는 적재하지 않는다. `results/legal_dong_code.json`이 수집 단계에서 이미
`{code, name}`으로 축약돼 있어 모델의 13개 컬럼(`region_cd`, `locatadd_nm` …)과 맞지 않는다.
채우려면 `collect_legal_dong_code.py`가 원본 필드 전체를 남기도록 고친 뒤 재수집해야 한다.
