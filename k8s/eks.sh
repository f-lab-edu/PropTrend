#!/usr/bin/env bash
# EKS에 이미지를 올리고 앱을 배포·정리한다. 인프라는 infra/의 Terraform이 만든다.
#
#   ./k8s/eks.sh push      # 이미지를 빌드해 ECR에 올린다 (infra/bootstrap apply 뒤)
#   ./k8s/eks.sh deploy    # 마지막으로 올린 이미지로 api와 Airflow를 배포한다 (infra/main apply 뒤)
#   ./k8s/eks.sh destroy   # 앱을 내리고 infra/main을 destroy한다
#
# Airflow UI: kubectl --context eks-prop-trend -n airflow port-forward svc/airflow-api-server 8080:8080
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
CTX=eks-prop-trend
BOOTSTRAP=$ROOT/infra/bootstrap
MAIN=$ROOT/infra/main
# push가 남기고 deploy가 읽는다. 빌드마다 태그가 바뀌어 두 명령 사이에 넘겨줄 곳이 필요하다.
TAG_FILE=$ROOT/infra/.image-tag
# shellcheck source=k8s/common.sh
source "$ROOT/k8s/common.sh"

push() {
    local region api_repo airflow_repo tag
    region=$(terraform -chdir="$BOOTSTRAP" output -raw region)
    api_repo=$(terraform -chdir="$BOOTSTRAP" output -raw api_repository_url)
    airflow_repo=$(terraform -chdir="$BOOTSTRAP" output -raw airflow_repository_url)
    # 커밋만으로는 커밋하지 않은 수정을 구분하지 못하고, 같은 태그면 노드가 이전 이미지를 그대로 쓴다.
    tag=$(git -C "$ROOT" rev-parse --short HEAD)-$(date +%Y%m%d%H%M%S)

    aws ecr get-login-password --region "$region" | docker login --username AWS --password-stdin "${api_repo%%/*}"
    # 노드가 Graviton(ARM)이다. Apple Silicon 맥이면 원래 arm64로 빌드되지만, 다른 환경에서도 같게 명시한다.
    docker build --platform linux/arm64 --build-context public="$ROOT/public" -t "$api_repo:$tag" "$ROOT/api"
    docker build --platform linux/arm64 -t "$airflow_repo:$tag" "$ROOT/airflow"
    docker push "$api_repo:$tag"
    docker push "$airflow_repo:$tag"
    echo "$tag" > "$TAG_FILE"
    echo "올린 태그: $tag"
}

deploy() {
    local region cluster tg api_repo airflow_repo tag
    region=$(terraform -chdir="$MAIN" output -raw region)
    cluster=$(terraform -chdir="$MAIN" output -raw cluster_name)
    tg=$(terraform -chdir="$MAIN" output -raw api_target_group_arn)
    api_repo=$(terraform -chdir="$BOOTSTRAP" output -raw api_repository_url)
    airflow_repo=$(terraform -chdir="$BOOTSTRAP" output -raw airflow_repository_url)
    if [[ ! -f "$TAG_FILE" ]]; then
        echo "올린 이미지가 없습니다. 먼저 $0 push를 실행하세요." >&2
        exit 1
    fi
    tag=$(cat "$TAG_FILE")

    aws eks update-kubeconfig --name "$cluster" --region "$region" --alias "$CTX"

    apply_api eks "$api_repo" "$tag" "patches:
  - target:
      kind: TargetGroupBinding
      name: api
    patch: |
      - op: replace
        path: /spec/targetGroupARN
        value: $tg"
    kubectl --context "$CTX" -n prop-trend rollout status deploy/api --timeout=300s

    install_airflow "$airflow_repo" "$tag" eks

    echo "앱 주소: $(terraform -chdir="$MAIN" output -raw app_url)"
}

destroy() {
    # TargetGroupBinding은 컨트롤러가 타깃을 내리고 finalizer를 풀어야 지워진다. 컨트롤러가 살아 있을 때 먼저 지운다.
    if kubectl --context "$CTX" get namespace prop-trend >/dev/null 2>&1; then
        kubectl --context "$CTX" -n prop-trend delete targetgroupbinding api --ignore-not-found --wait
        helm uninstall airflow --kube-context "$CTX" --namespace airflow --ignore-not-found --wait
    fi
    terraform -chdir="$MAIN" destroy
}

case "${1:-}" in
    push) push ;;
    deploy) deploy ;;
    destroy) destroy ;;
    *) echo "사용법: $0 push|deploy|destroy" >&2; exit 1 ;;
esac
