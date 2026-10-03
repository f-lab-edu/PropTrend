from datetime import datetime

from pydantic import EmailStr, Field

from . import PropTrendCoreModel


class SignUpRequest(PropTrendCoreModel):
    """회원가입 요청."""

    email: EmailStr = Field(max_length=255)
    nickname: str = Field(min_length=2, max_length=30)
    password: str = Field(min_length=8, max_length=64)


class UserResponse(PropTrendCoreModel):
    """비밀번호를 뺀 사용자 정보. 회원가입과 내 정보 조회가 함께 쓴다."""

    id: int
    email: str
    nickname: str
    updated_at: datetime
    created_at: datetime


class LoginRequest(PropTrendCoreModel):
    """로그인 요청."""

    email: EmailStr = Field(max_length=255)
    password: str
