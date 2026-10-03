# SQLAlchemy 2.1 기준 ORM CRUD 기본 패턴

**전제**: SQLAlchemy 2.1은 2.0과 기본 ORM 사용법(모델 정의, Session, select/insert/update/delete)이 거의 동일합니다. 주요 변경점은 아래에 별도로 정리했습니다. 코드는 실제로 `sqlalchemy==2.1.1`을 설치해 실행 검증했습니다.

## 1. 모델 정의 (Annotated Declarative)

```python
from typing import List, Optional
from sqlalchemy import ForeignKey, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "user_account"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(30))
    fullname: Mapped[Optional[str]]  # nullable

    addresses: Mapped[List["Address"]] = relationship(back_populates="user", cascade="all, delete-orphan")


class Address(Base):
    __tablename__ = "address"

    id: Mapped[int] = mapped_column(primary_key=True)
    email_address: Mapped[str]
    user_id: Mapped[int] = mapped_column(ForeignKey("user_account.id"))
    user: Mapped["User"] = relationship(back_populates="addresses")
```

## 2. 엔진/테이블 생성

```python
from sqlalchemy import create_engine

engine = create_engine("sqlite://", echo=True)
Base.metadata.create_all(engine)
```

## 3. Create — 객체 생성 후 Session으로 커밋

```python
from sqlalchemy.orm import Session

with Session(engine) as session:
    spongebob = User(
        name="spongebob",
        fullname="Spongebob Squarepants",
        addresses=[Address(email_address="spongebob@sqlalchemy.org")],
    )
    session.add_all([spongebob])
    session.commit()
```

대량 삽입 시엔 `insert()` + `Session.execute()`에 dict 리스트를 넘기는 **bulk INSERT** 방식도 가능(2.0에서 도입, 2.1도 동일):

```python
from sqlalchemy import insert

session.execute(
    insert(User),
    [
        {"name": "squidward", "fullname": "Squidward Tentacles"},
        {"name": "krabs", "fullname": "Eugene H. Krabs"},
    ],
)
```

## 4. Read — `select()` + `Session.scalars()`

```python
from sqlalchemy import select

stmt = select(User).where(User.name.in_(["spongebob", "sandy"]))
for user in session.scalars(stmt):
    print(user)

# JOIN
stmt = select(Address).join(Address.user).where(User.name == "sandy")
addr = session.scalars(stmt).one()
```

## 5. Update — 두 가지 방식

- **속성 변경(Unit of Work)**: 객체를 불러와 속성을 바꾸고 `commit()` → 자동으로 UPDATE 발생
```python
patrick = session.scalars(select(User).where(User.name == "patrick")).one()
patrick.fullname = "Patrick Star"
session.commit()
```
- **ORM-enabled UPDATE (WHERE 절 직접 지정, 여러 행 한 번에)**
```python
from sqlalchemy import update

stmt = update(User).where(User.name == "squidward").values(fullname="Squidward Q. Tentacles")
session.execute(stmt)
session.commit()
```

## 6. Delete — 두 가지 방식

- **`Session.delete()`**: 캐스케이드 규칙 적용, Unit of Work 기반
```python
user = session.get(User, 3)
session.delete(user)
session.commit()
```
- **ORM-enabled DELETE (WHERE 절, 대량 삭제)**
```python
from sqlalchemy import delete

session.execute(delete(User).where(User.name == "squidward"))
session.commit()
```

---

## 2.0 → 2.1에서 CRUD에 영향을 주는 변경점

- **Python 3.11부터 지원**(3.10 지원 종료)
- **PostgreSQL 기본 드라이버가 `psycopg2` → `psycopg`(psycopg3)로 변경** — PostgreSQL 사용 시 `create_engine("postgresql+psycopg://...")`를 명시하는 것을 권장
- **Session의 autoflush가 무조건 실행**되도록 단순화됨 (2.0에서는 ORM 문에서만 autoflush, 2.1부터는 `text()` 같은 Core 문 실행 시에도 autoflush 발생)
- **`filter_by()`가 FROM 절의 모든 엔티티를 검색** (이전엔 마지막 join된 엔티티/첫 FROM 엔티티만 검색). 컬럼명이 여러 엔티티에 겹치면 `AmbiguousColumnError` 발생 → 이 경우 `filter()`로 명시 필요
- **`MappedAsDataclass` 매핑에서 기본값(default) 처리 방식 변경**: `related_id`만 주고 `related`는 안 줬을 때 이전엔 `related=None`이 우선되어 FK가 NULL로 INSERT되는 버그가 있었는데, 2.1에서 `DONT_SET` 상수로 해결됨. `mapped_column(default=...)`을 쓰는 데이터클래스 매핑을 쓴다면 참고할 만합니다.
- **Composite 속성이 pending 객체에서 더 이상 자동으로 `None`을 반환하지 않음** (`composite()` 사용 시)

기본적인 CRUD만 쓴다면 2.0 코드가 거의 그대로 동작하며, 위 변경점 중 PostgreSQL 드라이버와 `filter_by()` 동작 변경이 실무에서 가장 자주 마주칠 이슈입니다.

Sources:
- [ORM Quick Start — SQLAlchemy 2.1 Documentation](https://docs.sqlalchemy.org/en/21/orm/quickstart.html)
- [ORM-Enabled INSERT, UPDATE, and DELETE statements — SQLAlchemy 2.1 Documentation](https://docs.sqlalchemy.org/en/21/orm/queryguide/dml.html)
- [What's New in SQLAlchemy 2.1? — SQLAlchemy 2.1 Documentation](https://docs.sqlalchemy.org/en/21/changelog/migration_21.html)