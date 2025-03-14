import abc
import datetime
import logging
import typing
from typing import TYPE_CHECKING

import pandas as pd
from sqlalchemy import JSON, Column, ForeignKey, Integer, create_engine
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    MappedAsDataclass,
    foreign,
    mapped_column,
    relationship,
    sessionmaker,
)
from sqlalchemy_history import make_versioned

from sports_planner_lib.metrics.calculate import (
    MetricsCalculator,
    get_metrics_map,
    parse_metric_string,
)

if TYPE_CHECKING:
    from sports_planner_lib.athlete import Athlete

logger = logging.getLogger(__name__)


class Base(MappedAsDataclass, DeclarativeBase):
    pass


class Record(Base):
    __tablename__ = "records"
    __versioned__ = {}

    timestamp: Mapped[datetime.datetime] = mapped_column(primary_key=True)
    activity_id: Mapped[int] = mapped_column(
        ForeignKey("activities.activity_id"), primary_key=True
    )
    latitude: Mapped[float | None] = mapped_column()
    longitude: Mapped[float | None] = mapped_column()
    distance: Mapped[float | None] = mapped_column()
    speed: Mapped[float | None] = mapped_column()
    altitude: Mapped[float | None] = mapped_column()
    power: Mapped[float | None] = mapped_column()
    vertical_oscillation: Mapped[float | None] = mapped_column()
    stance_time: Mapped[float | None] = mapped_column()
    vertical_ratio: Mapped[float | None] = mapped_column()
    step_length: Mapped[float | None] = mapped_column()
    heartrate: Mapped[int | None] = mapped_column()
    cadence: Mapped[int | None] = mapped_column()
    record_sequence: Mapped[int] = mapped_column()
    accumulated_power: Mapped[float | None] = mapped_column()
    performance_condition: Mapped[int | None] = mapped_column()
    activity = relationship("Activity", back_populates="records")


class Lap(Base):
    __tablename__ = "laps"
    __versioned__ = {}

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
    __versioned__ = {}

    index: Mapped[int] = mapped_column(primary_key=True)
    activity_id: Mapped[int] = mapped_column(
        ForeignKey("activities.activity_id"), primary_key=True
    )

    sport: Mapped[str] = mapped_column()
    sub_sport: Mapped[str] = mapped_column()

    activity = relationship("Activity", back_populates="sessions")


class UnknownMessage(Base):
    __tablename__ = "unknown_messages"
    __versioned__ = {}

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    activity_id: Mapped[int] = mapped_column(ForeignKey("activities.activity_id"))

    timestamp: Mapped[datetime.datetime | None] = mapped_column()
    type: Mapped[str] = mapped_column()
    record: dict[str, str | float | int] = mapped_column(JSON)

    activity = relationship("Activity", back_populates="unknown_messages")


class Metric(Base):
    __tablename__ = "metrics"
    __versioned__ = {}

    activity_id: Mapped[int] = mapped_column(
        ForeignKey("activities.activity_id"), primary_key=True
    )
    name: Mapped[str] = mapped_column(primary_key=True)
    value: Mapped[float | None] = mapped_column()
    json_value: Mapped[str | float | dict[str, str] | None] = mapped_column(JSON)

    activity = relationship("Activity", back_populates="metrics")


class MeanMax(Base):
    __tablename__ = "meanmaxes"
    __versioned__ = {}

    activity_id: Mapped[int] = mapped_column(
        ForeignKey("activities.activity_id"), primary_key=True
    )
    duration: Mapped[int] = mapped_column(primary_key=True)

    mean_max_speed: Mapped[float | None] = mapped_column()
    mean_max_power: Mapped[float | None] = mapped_column()
    mean_max_heartrate: Mapped[float | None] = mapped_column()
    mean_max_cadence: Mapped[float | None] = mapped_column()

    activity = relationship("Activity", back_populates="meanmaxes")


class Activity(Base):
    __tablename__ = "activities"
    __versioned__ = {}

    activity_id: Mapped[int] = mapped_column(primary_key=True)
    timestamp: Mapped[datetime.datetime] = mapped_column()
    total_timer_time: Mapped[float] = mapped_column()
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

    metrics = relationship(
        Metric,
        primaryjoin=activity_id == Metric.activity_id,
        back_populates="activity",
    )

    meanmaxes = relationship(
        MeanMax,
        primaryjoin=activity_id == MeanMax.activity_id,
        back_populates="activity",
    )

    def get_metric(self, name):
        logger.debug(f"getting {name} for {self.activity_id}")
        if isinstance(name, type):
            name = name.__name__
        for metric in self.metrics:
            if metric.name == name:
                if metric.value is not None:
                    return metric.value
                return metric.json_value

        if name in get_metrics_map():
            metric = get_metrics_map()[name]
            fields = []
        else:
            metric, fields = parse_metric_string(name)

        if metric is None:
            logger.error(f"{name} not found")
            return
        metric_instance = metric(self)
        if not metric_instance.get_applicable():
            logger.debug(f"{name} is not applicable")
            return None
        value = metric_instance.compute()
        for field in fields:
            try:
                value = value[field]
            except KeyError:
                value = None
                break
        return value

    _records_df = None

    @property
    def records_df(self):
        if self._records_df is None:
            df = pd.DataFrame([vars(record) for record in self.records])
            try:
                df.index = df["timestamp"]
                self._records_df = df
            except KeyError:
                print(df)
                raise KeyError
        return self._records_df

    @property
    def laps_df(self):
        df = pd.DataFrame([vars(lap) for lap in self.laps])
        return df

    @property
    def sessions_df(self):
        df = pd.DataFrame([vars(session) for session in self.sessions])
        return df

    @property
    def meanmaxes_df(self):
        df = pd.DataFrame([vars(meanmax) for meanmax in self.meanmaxes])
        return df


if __name__ == "__main__":
    engine = create_engine(
        "sqlite:////home/slaclau/sports-planner/seb.laclau@gmail.com/athlete.db"
    )
    Session = sessionmaker(bind=engine)
    Base.metadata.create_all(engine)
    make_versioned(user_cls=None)
