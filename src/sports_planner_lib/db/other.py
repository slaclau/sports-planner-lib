import datetime

from sqlalchemy.orm import Mapped, mapped_column

from sports_planner_lib.db.base import Base


class ConfiguredValue(Base):
    __versioned__ = {}
    __tablename__ = "configured_values"

    name: Mapped[str] = mapped_column(primary_key=True)
    sport: Mapped[str | None] = mapped_column(primary_key=True)
    date: Mapped[datetime.date] = mapped_column(primary_key=True)
    value: Mapped[float | None] = mapped_column()
