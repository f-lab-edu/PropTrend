# ALB를 컨트롤러(Ingress)가 아니라 Terraform이 만든다. 주소를 알아야 CloudFront origin으로 쓸 수 있고,
# destroy 때 컨트롤러가 만든 ALB가 남아 VPC 삭제를 막는 일이 없다. 파드 등록은 TargetGroupBinding이 한다.

# CloudFront가 원본에 접속할 때 쓰는 IP 대역. ALB를 CloudFront를 거쳐서만 열리게 한다.
data "aws_ec2_managed_prefix_list" "cloudfront" {
  name = "com.amazonaws.global.cloudfront.origin-facing"
}

resource "aws_security_group" "alb" {
  name        = "${var.name}-alb"
  description = "CloudFront to ALB"
  vpc_id      = aws_vpc.this.id
}

resource "aws_vpc_security_group_ingress_rule" "alb_from_cloudfront" {
  security_group_id = aws_security_group.alb.id
  prefix_list_id    = data.aws_ec2_managed_prefix_list.cloudfront.id
  ip_protocol       = "tcp"
  from_port         = 80
  to_port           = 80
}

resource "aws_vpc_security_group_egress_rule" "alb_to_api" {
  security_group_id            = aws_security_group.alb.id
  referenced_security_group_id = local.node_security_group_id
  ip_protocol                  = "tcp"
  from_port                    = 8000
  to_port                      = 8000
}

# 타깃이 파드 IP라 컨테이너 포트로 바로 들어간다. TargetGroupBinding의 networking을 비워 뒀으므로 이 규칙은 여기서만 관리한다.
resource "aws_vpc_security_group_ingress_rule" "node_from_alb" {
  security_group_id            = local.node_security_group_id
  referenced_security_group_id = aws_security_group.alb.id
  ip_protocol                  = "tcp"
  from_port                    = 8000
  to_port                      = 8000
}

resource "aws_lb" "api" {
  name               = "${var.name}-api"
  load_balancer_type = "application"
  security_groups    = [aws_security_group.alb.id]
  subnets            = aws_subnet.public[*].id
}

resource "aws_lb_target_group" "api" {
  name        = "${var.name}-api"
  port        = 8000
  protocol    = "HTTP"
  target_type = "ip"
  vpc_id      = aws_vpc.this.id
  # 기본 300초면 파드를 바꿀 때마다 이전 파드가 5분 동안 내려가지 않는다.
  deregistration_delay = 30

  health_check {
    path    = "/health"
    matcher = "200"
  }
}

resource "aws_lb_listener" "http" {
  load_balancer_arn = aws_lb.api.arn
  port              = 80
  protocol          = "HTTP"

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.api.arn
  }
}
