data "aws_availability_zones" "available" {
  state = "available"
}

locals {
  # EKS 클러스터와 RDS 서브넷 그룹 모두 AZ 두 개 이상을 요구한다.
  azs = slice(data.aws_availability_zones.available.names, 0, 2)
}

resource "aws_vpc" "this" {
  cidr_block = "10.0.0.0/16"
  # RDS 엔드포인트 같은 AWS 내부 DNS 이름을 VPC 안에서 풀려면 둘 다 켜야 한다.
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = { Name = var.name }
}

resource "aws_internet_gateway" "this" {
  vpc_id = aws_vpc.this.id

  tags = { Name = var.name }
}

# 노드와 ALB를 둔다. NAT Gateway(시간당 과금) 없이 노드가 공인 IP로 오픈API와 ECR에 나간다.
# 들어오는 트래픽은 보안그룹으로 ALB에서 오는 것만 받는다.
resource "aws_subnet" "public" {
  count = length(local.azs)

  vpc_id            = aws_vpc.this.id
  availability_zone = local.azs[count.index]
  # VPC CNI는 파드마다 서브넷 IP를 하나씩 쓰므로 넉넉하게 /20(4,096개)을 준다.
  cidr_block              = cidrsubnet(aws_vpc.this.cidr_block, 4, count.index)
  map_public_ip_on_launch = true

  tags = { Name = "${var.name}-public-${local.azs[count.index]}" }
}

# RDS만 둔다. 인터넷 경로가 없는 기본 라우트 테이블을 쓴다.
resource "aws_subnet" "private" {
  count = length(local.azs)

  vpc_id            = aws_vpc.this.id
  availability_zone = local.azs[count.index]
  cidr_block        = cidrsubnet(aws_vpc.this.cidr_block, 8, 200 + count.index)

  tags = { Name = "${var.name}-private-${local.azs[count.index]}" }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.this.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.this.id
  }

  tags = { Name = "${var.name}-public" }
}

resource "aws_route_table_association" "public" {
  count = length(aws_subnet.public)

  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}
