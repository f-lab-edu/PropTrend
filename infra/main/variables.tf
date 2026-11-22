variable "region" {
  description = "리소스를 만들 리전. backend 블록의 region과 같아야 한다."
  type        = string
  default     = "ap-northeast-2"
}

variable "name" {
  description = "리소스 이름 접두사"
  type        = string
  default     = "prop-trend"
}

variable "cluster_version" {
  description = "EKS 쿠버네티스 버전"
  type        = string
  default     = "1.37"
}

variable "node_instance_type" {
  description = "노드 인스턴스 타입. 맥(Apple Silicon)에서 빌드한 이미지를 그대로 쓰도록 ARM(Graviton)으로 둔다."
  type        = string
  default     = "t4g.large"
}

variable "node_count" {
  description = "노드 수. api, Airflow 3개 컴포넌트, 시스템 파드가 들어간다."
  type        = number
  default     = 2
}

variable "db_instance_class" {
  description = "RDS 인스턴스 클래스"
  type        = string
  default     = "db.t4g.micro"
}

variable "postgres_version" {
  description = "RDS PostgreSQL 메이저 버전. 로컬 compose의 postgres:18과 맞춘다. 마이너 버전은 RDS 기본값을 쓴다."
  type        = string
  default     = "18"
}

variable "cluster_public_access_cidrs" {
  description = "EKS API 엔드포인트에 접근할 수 있는 CIDR. 내 IP로 좁히려면 [\"x.x.x.x/32\"]."
  type        = list(string)
  default     = ["0.0.0.0/0"]
}
