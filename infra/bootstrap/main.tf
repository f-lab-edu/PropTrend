data "aws_caller_identity" "current" {}

# 버킷 이름은 전 세계에서 유일해야 해서 계정 ID를 붙인다.
resource "aws_s3_bucket" "state" {
  bucket = "${var.name}-tfstate-${data.aws_caller_identity.current.account_id}"
}

# state를 잘못 덮어써도 이전 버전으로 되돌릴 수 있게 한다.
resource "aws_s3_bucket_versioning" "state" {
  bucket = aws_s3_bucket.state.id

  versioning_configuration {
    status = "Enabled"
  }
}

# state에는 RDS 비밀번호 같은 값이 평문으로 들어간다.
resource "aws_s3_bucket_server_side_encryption_configuration" "state" {
  bucket = aws_s3_bucket.state.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "state" {
  bucket = aws_s3_bucket.state.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_ecr_repository" "this" {
  for_each = toset(["api", "airflow"])

  name = "${var.name}/${each.key}"
  # 실습 중 같은 태그로 다시 올릴 수 있게 한다. 배포 스크립트는 빌드마다 새 태그를 쓴다.
  image_tag_mutability = "MUTABLE"
  # 이미지가 남아 있어도 destroy로 지울 수 있게 한다.
  force_delete = true

  image_scanning_configuration {
    scan_on_push = true
  }
}

# 실습을 반복할수록 이미지가 쌓여 저장 비용이 늘어난다.
resource "aws_ecr_lifecycle_policy" "this" {
  for_each = aws_ecr_repository.this

  repository = each.value.name
  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "최근 이미지 5개만 남긴다"
      selection = {
        tagStatus   = "any"
        countType   = "imageCountMoreThan"
        countNumber = 5
      }
      action = { type = "expire" }
    }]
  })
}
