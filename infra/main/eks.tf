data "aws_iam_policy_document" "eks_assume" {
  statement {
    actions = ["sts:AssumeRole", "sts:TagSession"]

    principals {
      type        = "Service"
      identifiers = ["eks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "cluster" {
  name               = "${var.name}-eks-cluster"
  assume_role_policy = data.aws_iam_policy_document.eks_assume.json
}

resource "aws_iam_role_policy_attachment" "cluster" {
  role       = aws_iam_role.cluster.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSClusterPolicy"
}

resource "aws_eks_cluster" "this" {
  name     = var.name
  version  = var.cluster_version
  role_arn = aws_iam_role.cluster.arn

  vpc_config {
    subnet_ids              = aws_subnet.public[*].id
    endpoint_public_access  = true
    endpoint_private_access = true
    public_access_cidrs     = var.cluster_public_access_cidrs
  }

  # aws-auth ConfigMap 대신 access entry로 권한을 준다. 클러스터를 만든 IAM 주체(나)가 관리자가 되어
  # 같은 자격 증명으로 kubernetes·helm provider와 kubectl이 바로 붙는다.
  access_config {
    authentication_mode                         = "API"
    bootstrap_cluster_creator_admin_permissions = true
  }

  # 기본 애드온을 EKS가 자체 관리형으로 깔지 않게 한다. 아래 aws_eks_addon으로 직접 깔아 구성 요소를 드러낸다.
  bootstrap_self_managed_addons = false

  depends_on = [aws_iam_role_policy_attachment.cluster]
}

# 노드가 Ready가 되려면 네트워크(vpc-cni)가 먼저 있어야 한다. DaemonSet이라 노드 없이도 ACTIVE가 된다.
resource "aws_eks_addon" "before_nodes" {
  for_each = toset(["vpc-cni", "kube-proxy", "eks-pod-identity-agent"])

  cluster_name = aws_eks_cluster.this.name
  addon_name   = each.key
}

data "aws_iam_policy_document" "node_assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "node" {
  name               = "${var.name}-eks-node"
  assume_role_policy = data.aws_iam_policy_document.node_assume.json
}

resource "aws_iam_role_policy_attachment" "node" {
  for_each = toset([
    "arn:aws:iam::aws:policy/AmazonEKSWorkerNodePolicy",
    # vpc-cni가 노드 역할로 ENI와 IP를 붙인다. 파드 단위 권한(Pod Identity)으로 떼어 낼 수도 있지만 실습에선 단순하게 둔다.
    "arn:aws:iam::aws:policy/AmazonEKS_CNI_Policy",
    # 노드의 kubelet이 이 권한으로 ECR 이미지를 받아 imagePullSecrets가 필요 없다.
    "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryPullOnly",
  ])

  role       = aws_iam_role.node.name
  policy_arn = each.key
}

resource "aws_eks_node_group" "this" {
  cluster_name    = aws_eks_cluster.this.name
  node_group_name = "default"
  node_role_arn   = aws_iam_role.node.arn
  subnet_ids      = aws_subnet.public[*].id

  ami_type       = "AL2023_ARM_64_STANDARD"
  instance_types = [var.node_instance_type]
  capacity_type  = "ON_DEMAND"

  scaling_config {
    desired_size = var.node_count
    min_size     = var.node_count
    max_size     = var.node_count
  }

  update_config {
    max_unavailable = 1
  }

  depends_on = [aws_iam_role_policy_attachment.node, aws_eks_addon.before_nodes]
}

# Deployment라 노드가 없으면 파드가 뜨지 못해 애드온이 ACTIVE가 되지 않는다. 노드그룹 뒤에 만든다.
resource "aws_eks_addon" "coredns" {
  cluster_name = aws_eks_cluster.this.name
  addon_name   = "coredns"

  depends_on = [aws_eks_node_group.this]
}

locals {
  # 관리형 노드그룹은 시작 템플릿을 따로 주지 않으면 이 보안그룹을 노드에 붙인다.
  # 파드(VPC CNI)는 노드 ENI의 보안그룹을 그대로 쓰므로 ALB·RDS 규칙의 대상이 된다.
  node_security_group_id = aws_eks_cluster.this.vpc_config[0].cluster_security_group_id
}
