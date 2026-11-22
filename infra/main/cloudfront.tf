# 도메인 없이 HTTPS를 쓰려고 앞에 둔다. 세션 쿠키가 Secure라 HTTP로는 브라우저가 쿠키를 저장하지 않는다.
# CloudFront → ALB 구간은 ALB에 붙일 인증서(도메인)가 없어 HTTP다.
data "aws_cloudfront_cache_policy" "disabled" {
  name = "Managed-CachingDisabled"
}

# 쿠키·쿼리·헤더를 그대로 넘긴다. 로그인 세션 쿠키와 API 쿼리가 원본에 닿아야 한다.
data "aws_cloudfront_origin_request_policy" "all_viewer" {
  name = "Managed-AllViewer"
}

resource "aws_cloudfront_distribution" "api" {
  enabled         = true
  comment         = var.name
  is_ipv6_enabled = true
  # 한국을 포함한 아시아 엣지까지 쓴다. PriceClass_100은 북미·유럽만이다.
  price_class = "PriceClass_200"

  origin {
    origin_id   = "alb"
    domain_name = aws_lb.api.dns_name

    custom_origin_config {
      http_port              = 80
      https_port             = 443
      origin_protocol_policy = "http-only"
      origin_ssl_protocols   = ["TLSv1.2"]
    }
  }

  default_cache_behavior {
    target_origin_id       = "alb"
    viewer_protocol_policy = "redirect-to-https"
    allowed_methods        = ["GET", "HEAD", "OPTIONS", "PUT", "POST", "PATCH", "DELETE"]
    cached_methods         = ["GET", "HEAD"]
    # 실거래·로그인 응답을 엣지에 캐시하지 않는다. 정적 파일도 앱이 no-cache로 재검증을 요구한다.
    cache_policy_id          = data.aws_cloudfront_cache_policy.disabled.id
    origin_request_policy_id = data.aws_cloudfront_origin_request_policy.all_viewer.id
    compress                 = true
  }

  restrictions {
    geo_restriction {
      restriction_type = "none"
    }
  }

  viewer_certificate {
    cloudfront_default_certificate = true
  }
}
