variable "region" {
  description = "리소스를 만들 리전. 공공데이터포털이 해외 IP를 막을 수 있어 서울을 쓴다."
  type        = string
  default     = "ap-northeast-2"
}

variable "name" {
  description = "리소스 이름 접두사"
  type        = string
  default     = "prop-trend"
}
