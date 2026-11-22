# 서비스 DB(prop_trend)와 Airflow 메타 DB(airflow)를 인스턴스 하나에 둔다. compose는 커넥션 예산과
# 백업 범위를 나누려고 둘을 분리하지만, 실습에서는 인스턴스 비용과 생성 시간을 줄이는 쪽을 택했다.
# airflow DB는 k8s/airflow/create-db-job.yaml이 만든다. RDS는 생성 시 DB를 하나만 만든다.

# 접속 URL에 그대로 들어가므로 URL에 안전한 문자만 쓴다.
resource "random_password" "db" {
  length  = 32
  special = false
}

resource "aws_db_subnet_group" "this" {
  name       = var.name
  subnet_ids = aws_subnet.private[*].id
}

resource "aws_security_group" "db" {
  name        = "${var.name}-db"
  description = "EKS pods to RDS"
  vpc_id      = aws_vpc.this.id
}

resource "aws_vpc_security_group_ingress_rule" "db_from_nodes" {
  security_group_id            = aws_security_group.db.id
  referenced_security_group_id = local.node_security_group_id
  ip_protocol                  = "tcp"
  from_port                    = 5432
  to_port                      = 5432
}

# compose의 postgres는 TZ=Asia/Seoul로 초기화된다. 세션 타임존이 달라 SQL의 날짜 계산이 어긋나지 않게 맞춘다.
resource "aws_db_parameter_group" "this" {
  name   = var.name
  family = "postgres${var.postgres_version}"

  parameter {
    name  = "timezone"
    value = "Asia/Seoul"
  }
}

resource "aws_db_instance" "this" {
  identifier     = var.name
  engine         = "postgres"
  engine_version = var.postgres_version
  instance_class = var.db_instance_class

  allocated_storage = 20
  storage_type      = "gp3"
  # 추가 비용 없이 디스크를 암호화한다. 생성 뒤에는 바꿀 수 없어 처음부터 켠다.
  storage_encrypted = true

  db_name  = "prop_trend"
  username = "postgres"
  password = random_password.db.result

  db_subnet_group_name   = aws_db_subnet_group.this.name
  vpc_security_group_ids = [aws_security_group.db.id]
  parameter_group_name   = aws_db_parameter_group.this.name
  publicly_accessible    = false
  multi_az               = false

  # 실습 데이터는 다시 받으면 된다. 백업과 최종 스냅샷을 끄면 생성·삭제가 빨라지고 스냅샷 비용이 없다.
  backup_retention_period = 0
  skip_final_snapshot     = true
  deletion_protection     = false
  apply_immediately       = true
}
