# 앱이 읽는 네임스페이스와 Secret. DB 비밀번호는 Terraform만 알기 때문에 여기서 만든다.
# 앱 자체(Deployment, Airflow 차트)는 k8s/eks.sh가 배포한다. 오픈API 인증키도 state에 남지 않도록 그쪽에서 넣는다.

locals {
  db_auth = "${aws_db_instance.this.username}:${random_password.db.result}@${aws_db_instance.this.address}:${aws_db_instance.this.port}"
}

# destroy 순서를 위해 컨트롤러에 의존한다. 네임스페이스를 먼저 지워야 TargetGroupBinding의 finalizer를
# 컨트롤러가 처리하고, 반대면 네임스페이스 삭제가 finalizer에 걸려 멈춘다.
resource "kubernetes_namespace_v1" "this" {
  for_each = toset(["prop-trend", "airflow"])

  metadata {
    name = each.key
    # api는 replica가 하나라, 새 파드가 ALB 헬스체크를 통과하기 전에 기존 파드가 내려가면 타깃이 빈다.
    # readiness gate로 ALB에서 healthy가 된 뒤에 Ready가 되게 한다. 컨트롤러 웹훅은 TargetGroupBinding이
    # 이미 있을 때 만든 파드에만 gate를 붙여 첫 배포에는 빠지지만, 그때는 넘겨받을 트래픽이 없다.
    labels = each.key == "prop-trend" ? { "elbv2.k8s.aws/pod-readiness-gate-inject" = "enabled" } : {}
  }

  depends_on = [helm_release.load_balancer_controller]
}

# RDS PostgreSQL 15부터 rds.force_ssl이 기본으로 켜져 있어 모든 접속에 TLS를 요구한다.
resource "kubernetes_secret_v1" "api_db" {
  metadata {
    name      = "api-db"
    namespace = kubernetes_namespace_v1.this["prop-trend"].metadata[0].name
  }

  data = {
    DATABASE_URL = "postgresql+asyncpg://${local.db_auth}/prop_trend?ssl=require"
  }
}

resource "kubernetes_secret_v1" "airflow_metadata" {
  metadata {
    name      = "airflow-metadata"
    namespace = kubernetes_namespace_v1.this["airflow"].metadata[0].name
  }

  data = {
    connection = "postgresql+psycopg2://${local.db_auth}/airflow?sslmode=require"
  }
}

resource "kubernetes_secret_v1" "airflow_connections" {
  metadata {
    name      = "airflow-connections"
    namespace = kubernetes_namespace_v1.this["airflow"].metadata[0].name
  }

  data = {
    AIRFLOW_CONN_PROP_TREND_DB = "postgresql://${local.db_auth}/prop_trend?sslmode=require"
  }
}

# create-db-job.yaml이 libpq 환경변수로 읽는다.
resource "kubernetes_secret_v1" "db_admin" {
  metadata {
    name      = "db-admin"
    namespace = kubernetes_namespace_v1.this["airflow"].metadata[0].name
  }

  data = {
    PGHOST     = aws_db_instance.this.address
    PGPORT     = tostring(aws_db_instance.this.port)
    PGUSER     = aws_db_instance.this.username
    PGPASSWORD = random_password.db.result
    PGSSLMODE  = "require"
  }
}
