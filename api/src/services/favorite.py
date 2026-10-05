from sqlalchemy import delete
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from ..exceptions import ComplexNotFoundError
from ..model.complex import Complex
from ..model.favorite import FavoriteComplex
from ..schemas.favorite import FavoriteComplexToggleResponse


async def toggle_favorite_complex(
    session: AsyncSession, user_id: int, complex_id: int
) -> FavoriteComplexToggleResponse:
    """사용자의 단지 즐겨찾기가 있으면 제거하고 없으면 추가한다."""
    if await session.get(Complex, complex_id) is None:
        raise ComplexNotFoundError("단지를 찾을 수 없습니다")

    deleted = await session.scalar(
        delete(FavoriteComplex)
        .where(FavoriteComplex.user_id == user_id, FavoriteComplex.complex_id == complex_id)
        .returning(FavoriteComplex.complex_id)
    )
    if deleted is not None:
        return FavoriteComplexToggleResponse(complex_id=complex_id, is_favorite=False)

    # 연속 클릭으로 같은 요청이 동시에 들어와 둘 다 추가로 판단해도 기본키 충돌로 500이 나지 않게 한다.
    await session.execute(
        insert(FavoriteComplex).values(user_id=user_id, complex_id=complex_id).on_conflict_do_nothing()
    )
    return FavoriteComplexToggleResponse(complex_id=complex_id, is_favorite=True)
