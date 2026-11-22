#!/usr/bin/env bash
# 로컬 kind 클러스터에 api와 Airflow를 올린다. EKS에 올리기 전에 매니페스트와 차트 값을 비용 없이 검증한다.
#
#   ./k8s/kind.sh up     # 클러스터 생성, 이미지 빌드·적재, 배포 (다시 실행하면 이미지만 새로 넣고 갱신한다)
#   ./k8s/kind.sh down   # 클러스터 삭제
#
# 접속: kubectl --context kind-prop-trend -n prop-trend port-forward svc/api 8000:80
#       kubectl --context kind-prop-trend -n airflow port-forward svc/airflow-api-server 8080:8080
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
CLUSTER=prop-trend
CTX=kind-$CLUSTER
# 같은 태그를 다시 쓰면 노드에 있는 이전 이미지로 파드가 뜬다. 빌드마다 태그를 바꿔 새 이미지로 교체되게 한다.
TAG=dev-$(date +%Y%m%d%H%M%S)
# shellcheck source=k8s/common.sh
source "$ROOT/k8s/common.sh"

up() {
    kind get clusters | grep -qx "$CLUSTER" || kind create cluster --config "$ROOT/k8s/kind-config.yaml"

    docker build --build-context public="$ROOT/public" -t "prop-trend-api:$TAG" "$ROOT/api"
    docker build -t "prop-trend-airflow:$TAG" "$ROOT/airflow"
    kind load docker-image --name "$CLUSTER" "prop-trend-api:$TAG" "prop-trend-airflow:$TAG"

    apply_api kind prop-trend-api "$TAG"
    kubectl --context "$CTX" -n prop-trend rollout status deploy/postgres --timeout=180s
    kubectl --context "$CTX" -n prop-trend rollout status deploy/api --timeout=180s

    # EKS에서는 Terraform이 만드는 Secret들이다. 로컬 전용 DB라 비밀번호를 그대로 쓴다.
    local db=postgres:postgres@postgres.prop-trend.svc.cluster.local:5432
    kubectl --context "$CTX" create namespace airflow --dry-run=client -o yaml | kubectl --context "$CTX" apply -f -
    kubectl --context "$CTX" -n airflow create secret generic db-admin \
        --from-literal=PGHOST=postgres.prop-trend.svc.cluster.local \
        --from-literal=PGUSER=postgres --from-literal=PGPASSWORD=postgres \
        --dry-run=client -o yaml | kubectl --context "$CTX" apply -f -
    kubectl --context "$CTX" -n airflow create secret generic airflow-metadata \
        --from-literal=connection="postgresql+psycopg2://$db/airflow" \
        --dry-run=client -o yaml | kubectl --context "$CTX" apply -f -
    kubectl --context "$CTX" -n airflow create secret generic airflow-connections \
        --from-literal=AIRFLOW_CONN_PROP_TREND_DB="postgresql://$db/prop_trend" \
        --dry-run=client -o yaml | kubectl --context "$CTX" apply -f -

    install_airflow prop-trend-airflow "$TAG" kind
}

down() {
    kind delete cluster --name "$CLUSTER"
}

case "${1:-}" in
    up) up ;;
    down) down ;;
    *) echo "사용법: $0 up|down" >&2; exit 1 ;;
esac
