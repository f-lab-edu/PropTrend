#!/usr/bin/env bash
# kind.sh와 eks.sh가 함께 쓰는 Airflow 배포 단계. 직접 실행하지 않고 source한다.
# 호출하는 쪽이 ROOT(저장소 루트)와 CTX(kubectl 컨텍스트)를 정해 둔다.

AIRFLOW_CHART_VERSION=1.22.0

# api 매니페스트를 이미지 태그를 정해 적용한다. $1: overlay 이름(kind|eks), $2: 이미지 저장소, $3: 태그, $4: 덧붙일 kustomization 내용
# 태그는 배포할 때마다 바뀌어 overlay에 적어 둘 수 없다. 저장소 파일을 고치지 않도록 임시 overlay를 만든다.
# kustomize는 절대 경로나 다른 위치의 base를 받지 않아 임시 디렉터리를 k8s/ 아래에 둔다(.gitignore 대상).
apply_api() {
    local dir
    dir=$(mktemp -d "$ROOT/k8s/.deploy.XXXXXX")
    cat > "$dir/kustomization.yaml" <<YAML
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - ../overlays/$1
images:
  - name: prop-trend-api
    newName: $2
    newTag: $3
${4:-}
YAML
    local status=0
    kubectl --context "$CTX" apply -k "$dir" || status=$?
    rm -rf "$dir"
    return "$status"
}

# 공공데이터포털 인증키를 airflow/.env에서 읽어 Connection Secret으로 넣는다.
apply_data_go_kr_secret() {
    local key
    # .env를 source하면 그 안의 내용이 셸 코드로 실행되므로 해당 줄만 읽는다.
    key=$(sed -n 's/^DATA_GO_KR_SERVICE_KEY=//p' "$ROOT/airflow/.env" | tail -n 1)
    # compose는 값을 감싼 따옴표를 떼고 읽는다. 같은 .env를 쓰므로 똑같이 뗀다.
    if [[ $key =~ ^\"(.*)\"$ || $key =~ ^\'(.*)\'$ ]]; then
        key=${BASH_REMATCH[1]}
    fi
    if [[ -z "$key" ]]; then
        echo "airflow/.env에 DATA_GO_KR_SERVICE_KEY를 넣어야 합니다." >&2
        return 1
    fi
    # 키에 /·+·=가 있어 URI가 아닌 JSON 형식을 쓴다. password에 둬야 태스크 로그에서 가려진다.
    # 문자열을 이어 붙이면 키에 따옴표나 역슬래시가 있을 때 JSON이 깨져 jq로 인코딩한다.
    local conn
    conn=$(printf '%s' "$key" | jq -cR '{conn_type: "http", host: "https://apis.data.go.kr", password: .}')
    kubectl --context "$CTX" -n airflow create secret generic data-go-kr \
        --from-literal=AIRFLOW_CONN_DATA_GO_KR="$conn" \
        --dry-run=client -o yaml | kubectl --context "$CTX" apply -f -
}

# 없을 때만 임의 값으로 Secret을 만든다. $1: 이름, $2: 키
# 차트에 맡기면 helm upgrade마다 값을 새로 만들어 컴포넌트가 재시작되고 이미 발급한 토큰이 무효가 된다.
ensure_random_secret() {
    kubectl --context "$CTX" -n airflow get secret "$1" >/dev/null 2>&1 && return 0
    kubectl --context "$CTX" -n airflow create secret generic "$1" --from-literal="$2=$(openssl rand -hex 32)"
}

# 메타 DB를 만들고 Airflow 차트를 설치한다. $1: 이미지 저장소, $2: 태그, $3: 환경(kind|eks)
install_airflow() {
    local repository=$1 tag=$2 env=$3
    apply_data_go_kr_secret
    ensure_random_secret api-secret-key api-secret-key
    ensure_random_secret jwt-secret jwt-secret

    # Job의 spec은 바꿀 수 없어 지우고 다시 만든다.
    kubectl --context "$CTX" -n airflow delete job create-airflow-db --ignore-not-found
    kubectl --context "$CTX" -n airflow apply -f "$ROOT/k8s/airflow/create-db-job.yaml"
    kubectl --context "$CTX" -n airflow wait --for=condition=complete job/create-airflow-db --timeout=300s

    # 훅을 끈 마이그레이션 Job도 같은 이유로 이미지 태그가 바뀌면 upgrade가 실패한다. 차트가 다시 만든다.
    kubectl --context "$CTX" -n airflow delete job airflow-run-airflow-migrations --ignore-not-found

    helm repo add apache-airflow https://airflow.apache.org --force-update >/dev/null
    helm upgrade --install airflow apache-airflow/airflow \
        --kube-context "$CTX" --namespace airflow \
        --version "$AIRFLOW_CHART_VERSION" \
        -f "$ROOT/k8s/airflow/values.yaml" \
        -f "$ROOT/k8s/airflow/values-$env.yaml" \
        --set images.airflow.repository="$repository" \
        --set images.airflow.tag="$tag" \
        --wait --timeout 15m
}
