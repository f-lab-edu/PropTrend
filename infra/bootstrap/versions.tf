# main의 state를 담을 버킷과 이미지 저장소. 실습 사이클(apply → destroy)마다 지우지 않고 남겨 둔다.
# 자기 state는 담을 곳이 아직 없으므로 로컬(terraform.tfstate, .gitignore 대상)에 둔다.
terraform {
  required_version = ">= 1.10"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.73"
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
