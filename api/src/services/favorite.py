from sqlalchemy import delete, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from ..exceptions import ComplexNotFoundError
from ..model.complex import Complex
from ..model.favorite import FavoriteComplex
from ..schemas.favorite import FavoriteComplexAddResponse, FavoriteComplexResponse
from .prop_transaction import get_region_names


async def add_favorite_complex(session: AsyncSession, user_id: int, complex_id: int) -> FavoriteComplexAddResponse:
    """사용자의 단지 즐겨찾기를 추가한다. 이미 있으면 그대로 둔다."""
    if await session.get(Complex, complex_id) is None:
        raise ComplexNotFoundError("단지를 찾을 수 없습니다")

    # 연속 클릭으로 같은 요청이 동시에 들어와도 기본키 충돌로 500이 나지 않게 한다.
    await session.execute(
        insert(FavoriteComplex).values(user_id=user_id, complex_id=complex_id).on_conflict_do_nothing()
    )
    return FavoriteComplexAddResponse(complex_id=complex_id)


async def remove_favorite_complex(session: AsyncSession, user_id: int, complex_id: int) -> None:
    """사용자의 단지 즐겨찾기를 제거한다. 없으면 아무것도 하지 않는다."""
    await session.execute(
        delete(FavoriteComplex).where(FavoriteComplex.user_id == user_id, FavoriteComplex.complex_id == complex_id)
    )


async def get_favorite_complexes(session: AsyncSession, user_id: int) -> list[FavoriteComplexResponse]:
    """사용자가 즐겨찾기한 단지를 최근에 추가한 순으로 조회한다."""
    result = await session.execute(
        select(FavoriteComplex.created_at, Complex)
        .join(Complex, Complex.id == FavoriteComplex.complex_id)
        .where(FavoriteComplex.user_id == user_id)
        .order_by(FavoriteComplex.created_at.desc(), Complex.id.desc())
    )
    names = await get_region_names(session)
    return [
        FavoriteComplexResponse(
            complex_id=complex_.id,
            property_type=complex_.property_type,
            building_name=complex_.building_name,
            build_year=complex_.build_year,
            favorited_at=favorited_at,
            region_name=names.get((complex_.sido_code, complex_.sigungu_code)),
            umd_name=complex_.umd_name,
            jibun=complex_.jibun,
        )
        for favorited_at, complex_ in result.tuples()
    ]
