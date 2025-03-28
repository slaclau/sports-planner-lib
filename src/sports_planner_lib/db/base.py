from sqlalchemy.orm import (
    DeclarativeBase,
    MappedAsDataclass,
)
from sqlalchemy_history import make_versioned


class _Base(DeclarativeBase):
    pass


class Base(MappedAsDataclass, _Base):
    __abstract__ = True


make_versioned(user_cls=None, options=dict(base_classes=(_Base,)))
