# 실습 환경 전체. 1~2시간 띄웠다가 destroy하는 것을 전제로, 비용과 삭제 속도를 우선한 설정이 있다.
#
#   terraform init -backend-config="bucket=$(terraform -chdir=../bootstrap output -raw state_bucket)"
terraform {
  required_version = ">= 1.10"

  # 버킷 이름에 계정 ID가 들어가 여기 적지 않고 init 때 준다. 잠금은 S3 조건부 쓰기로 해 DynamoDB가 필요 없다.
  backend "s3" {
    key          = "main/terraform.tfstate"
    region       = "ap-northeast-2"
    use_lockfile = true
    encrypt      = true
  }

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.73"
    }
    helm = {
      source  = "hashicorp/helm"
      version = "~> 3.3"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 3.5"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.9"
    }
  }
}

provider "aws" {
  region = var.region

  default_tags {
    tags = {
      Project   = var.name
      ManagedBy = "terraform"
    }
  }
}

# 같은 apply에서 만든 클러스터에 붙는다. 토큰은 만료가 짧아 고정값 대신 매번 aws CLI로 받는다.
locals {
  cluster_exec = {
    api_version = "client.authentication.k8s.io/v1beta1"
    command     = "aws"
    args        = ["eks", "get-token", "--cluster-name", aws_eks_cluster.this.name, "--region", var.region]
  }
}

provider "kubernetes" {
  host                   = aws_eks_cluster.this.endpoint
  cluster_ca_certificate = base64decode(aws_eks_cluster.this.certificate_authority[0].data)

  exec {
    api_version = local.cluster_exec.api_version
    command     = local.cluster_exec.command
    args        = local.cluster_exec.args
  }
}

provider "helm" {
  kubernetes = {
    host                   = aws_eks_cluster.this.endpoint
    cluster_ca_certificate = base64decode(aws_eks_cluster.this.certificate_authority[0].data)
    exec                   = local.cluster_exec
  }
}
