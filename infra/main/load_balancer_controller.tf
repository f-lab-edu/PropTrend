# TargetGroupBinding으로 api 파드 IP를 ALB 타깃그룹에 등록하는 컨트롤러.
# 정책은 컨트롤러 저장소의 docs/install/iam_policy.json(v3.6.3)을 그대로 옮겼다. 차트 버전을 올리면 함께 바꾼다.
resource "aws_iam_policy" "load_balancer_controller" {
  name   = "${var.name}-aws-load-balancer-controller"
  policy = file("${path.module}/policies/aws-load-balancer-controller.json")
}

# Pod Identity: 노드 역할을 넓히지 않고 이 ServiceAccount의 파드에만 권한을 준다.
data "aws_iam_policy_document" "pod_identity_assume" {
  statement {
    actions = ["sts:AssumeRole", "sts:TagSession"]

    principals {
      type        = "Service"
      identifiers = ["pods.eks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "load_balancer_controller" {
  name               = "${var.name}-aws-load-balancer-controller"
  assume_role_policy = data.aws_iam_policy_document.pod_identity_assume.json
}

resource "aws_iam_role_policy_attachment" "load_balancer_controller" {
  role       = aws_iam_role.load_balancer_controller.name
  policy_arn = aws_iam_policy.load_balancer_controller.arn
}

resource "aws_eks_pod_identity_association" "load_balancer_controller" {
  cluster_name    = aws_eks_cluster.this.name
  namespace       = "kube-system"
  service_account = "aws-load-balancer-controller"
  role_arn        = aws_iam_role.load_balancer_controller.arn
}

resource "helm_release" "load_balancer_controller" {
  name       = "aws-load-balancer-controller"
  repository = "https://aws.github.io/eks-charts"
  chart      = "aws-load-balancer-controller"
  version    = "3.6.3"
  namespace  = "kube-system"

  values = [yamlencode({
    clusterName = aws_eks_cluster.this.name
    # 주지 않으면 노드의 인스턴스 메타데이터에서 찾는데, 파드에서 IMDS 접근이 막혀 있으면 기동에 실패한다.
    region       = var.region
    vpcId        = aws_vpc.this.id
    replicaCount = 1
    serviceAccount = {
      create = true
      name   = "aws-load-balancer-controller"
    }
    # type: LoadBalancer Service를 쓰지 않는다. 켜 두면 모든 Service 생성이 이 웹훅을 거친다.
    enableServiceMutatorWebhook = false
  })]

  depends_on = [
    aws_eks_pod_identity_association.load_balancer_controller,
    aws_iam_role_policy_attachment.load_balancer_controller,
    aws_eks_addon.coredns,
  ]
}
