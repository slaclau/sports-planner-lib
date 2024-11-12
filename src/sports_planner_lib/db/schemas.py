import datetime
import typing
from typing import TYPE_CHECKING

import pandas as pd
from sqlalchemy import Column, ForeignKey, Integer, create_engine, JSON
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    MappedAsDataclass,
    foreign,
    mapped_column,
    relationship,
    sessionmaker,
)

from sports_planner_lib.metrics.activity import CurveMeta, MeanMaxMeta
from sports_planner_lib.metrics.zones import (
    TimeInZoneMeta,
    ZoneDefinitionsMeta,
    ZonesMeta,
)
from sports_planner_lib.metrics.calculate import MetricsCalculator, get_metrics_map

if TYPE_CHECKING:
    from sports_planner_lib.athlete import Athlete


class Base(MappedAsDataclass, DeclarativeBase):
    pass


class Record(Base):
    __tablename__ = "records"

    timestamp: Mapped[datetime.datetime] = mapped_column(primary_key=True)
    activity_id: Mapped[int] = mapped_column(
        ForeignKey("activities.activity_id"), primary_key=True
    )
    latitude: Mapped[float] = mapped_column()
    longitude: Mapped[float] = mapped_column()
    distance: Mapped[float] = mapped_column()
    speed: Mapped[float] = mapped_column()
    altitude: Mapped[float] = mapped_column()
    power: Mapped[float] = mapped_column()
    vertical_oscillation: Mapped[float | None] = mapped_column()
    stance_time: Mapped[float | None] = mapped_column()
    vertical_ratio: Mapped[float | None] = mapped_column()
    step_length: Mapped[float | None] = mapped_column()
    heartrate: Mapped[int] = mapped_column()
    cadence: Mapped[int] = mapped_column()
    record_sequence: Mapped[int] = mapped_column()
    accumulated_power: Mapped[float | None] = mapped_column()
    performance_condition: Mapped[int | None] = mapped_column()
    activity = relationship("Activity", back_populates="records")


class Lap(Base):
    __tablename__ = "laps"

    index: Mapped[int] = mapped_column(primary_key=True)
    activity_id: Mapped[int] = mapped_column(
        ForeignKey("activities.activity_id"), primary_key=True
    )
    start_time: Mapped[datetime.datetime] = mapped_column()

    start_position_lat: Mapped[float] = mapped_column()
    start_position_long: Mapped[float] = mapped_column()
    end_position_lat: Mapped[float] = mapped_column()
    end_position_long: Mapped[float] = mapped_column()

    total_elapsed_time: Mapped[float] = mapped_column()
    total_elapsed_time: Mapped[float] = mapped_column()

    activity = relationship("Activity", back_populates="laps")


class Session(Base):
    __tablename__ = "sessions"

    index: Mapped[int] = mapped_column(primary_key=True)
    activity_id: Mapped[int] = mapped_column(
        ForeignKey("activities.activity_id"), primary_key=True
    )

    sport: Mapped[str] = mapped_column()
    sub_sport: Mapped[str] = mapped_column()

    activity = relationship("Activity", back_populates="sessions")


class UnknownMessage(Base):
    __tablename__ = "unknown_messages"

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    activity_id: Mapped[int] = mapped_column(ForeignKey("activities.activity_id"))

    timestamp: Mapped[datetime.datetime | None] = mapped_column()
    type: Mapped[str] = mapped_column()
    record: dict[str, str | float | int] = mapped_column(JSON)

    activity = relationship("Activity", back_populates="unknown_messages")


class Metric(Base):
    __tablename__ = "metrics"

    activity_id: Mapped[int] = mapped_column(
        ForeignKey("activities.activity_id"), primary_key=True
    )
    name: Mapped[str] = mapped_column(primary_key=True)
    value: Mapped[str | float | dict[str, str]] = mapped_column(JSON)

    activity = relationship("Activity", back_populates="_metrics")


class Activity(Base):
    __tablename__ = "activities"

    activity_id: Mapped[int] = mapped_column(primary_key=True)
    timestamp: Mapped[datetime.datetime] = mapped_column()
    name: Mapped[str] = mapped_column()
    source: Mapped[str] = mapped_column()
    original_file: Mapped[str] = mapped_column()

    records = relationship(
        Record,
        primaryjoin=activity_id == Record.activity_id,
        back_populates="activity",
    )

    laps = relationship(
        Lap,
        primaryjoin=activity_id == Lap.activity_id,
        back_populates="activity",
    )

    sessions = relationship(
        Session,
        primaryjoin=activity_id == Session.activity_id,
        back_populates="activity",
    )

    unknown_messages = relationship(
        UnknownMessage,
        primaryjoin=activity_id == UnknownMessage.activity_id,
        back_populates="activity",
    )

    _metrics = relationship(
        Metric,
        primaryjoin=activity_id == Metric.activity_id,
        back_populates="activity",
    )

    @property
    def metrics(self):
        return {metric.name: metric.value for metric in self._metrics}

    def get_metric(self, name):
        return self.metrics[name]

    @property
    def records_df(self):
        df = pd.DataFrame([vars(record) for record in self.records])
        df.index = df["timestamp"]
        return df

    @property
    def laps_df(self):
        df = pd.DataFrame([vars(lap) for lap in self.laps])
        return df

    @property
    def sessions_df(self):
        df = pd.DataFrame([vars(session) for session in self.sessions])
        return df


if __name__ == "__main__":
    engine = create_engine(
        "sqlite:////home/slaclau/sports-planner/seb.laclau@gmail.com/athlete.db"
    )
    Session = sessionmaker(bind=engine)
    Base.metadata.create_all(engine)
