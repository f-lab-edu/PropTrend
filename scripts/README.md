# scripts

국토교통부/행정안전부 공공데이터를 수집하고, 수집한 원본을 DB에 적재하는 스크립트 모음.

## 수집

`scripts/results` 아래에 원본 응답을 그대로 남긴다. `.env`에 `DATA_GO_KR_SERVICE_KEY`가 필요하다.

```bash
uv run --project scripts python scripts/src/collect_legal_dong_code.py
uv run --project scripts python scripts/src/collect_rtms.py
```

## 적재 (`load_results.py`)

`scripts/results`에 쌓인 월별 원본 JSON을 가공해 DB에 적재하는 **일회성 배치**다. 가공/적재
로직과 모델이 `api` 프로젝트에 있으므로 **api 가상환경으로 실행**한다.

```bash
docker compose up -d postgres

# 시범 적재
uv run --project api python scripts/src/load_results.py --api officetel_sale --from 202501 --to 202506

# 전체 적재 (약 35분, 백그라운드 권장)
uv run --project api python scripts/src/load_results.py 2>&1 | tee /tmp/load_results.log
```

테이블이 없으면 먼저 만들고(`Base.metadata.create_all`), 법정동코드 마스터를 적재한 뒤 8종
실거래가를 월 오름차순으로 넣는다. 접속 정보는 `DATABASE_URL`로 덮어쓸 수 있다(기본값은
`docker-compose.yml`과 동일).

| 옵션 | 설명 |
|---|---|
| `--api <id>` | 특정 API만 적재. 여러 번 지정 가능, 생략 시 8종 전부 |
| `--from YYYYMM` / `--to YYYYMM` | 계약년월 범위 제한 |
| `--dry-run` | 가공까지만 하고 적재는 생략 |
| `--restart` | 이전 진행 기록을 무시하고 처음부터 |
| `--skip-legal-dong-code` | 법정동코드 마스터 적재 생략 |
| `--no-create-tables` | 테이블 생성 생략 |

적재를 마친 파일은 `scripts/results/load_results.progress.json`에 기록되어, 중단 후 다시
실행하면 남은 파일부터 이어간다. 적재 자체가 갱신 단위(부동산 유형, 시군구, 계약년월)로
멱등하므로 같은 파일을 다시 넣어도 결과는 같다.
