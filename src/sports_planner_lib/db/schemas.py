import datetime
import typing

import pandas as pd
from sqlalchemy import Column, ForeignKey, Integer, create_engine
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
    session: Mapped[int] = mapped_column()
    lap: Mapped[int] = mapped_column()

    activity = relationship("Activity", back_populates="records")


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

    metrics = {}

    @property
    def records_df(self):
        df = pd.DataFrame([vars(record) for record in self.records])
        df.index = df["timestamp"]
        return df

    def get_metric(self, metric: type["Metric"] | str):
        if isinstance(metric, str):
            metric = get_metrics_map()[metric]
        try:
            if metric.__class__ in [
                CurveMeta,
                MeanMaxMeta,
                TimeInZoneMeta,
                ZoneDefinitionsMeta,
                ZonesMeta,
            ]:
                return MetricsCalculator(self, [metric]).metrics[metric.name]
            return MetricsCalculator(self, [metric]).metrics[metric]
        except KeyError:
            return


if __name__ == "__main__":
    engine = create_engine(
        "sqlite:////home/slaclau/sports-planner/seb.laclau@gmail.com/athlete.db"
    )
    Session = sessionmaker(bind=engine)
    Base.metadata.create_all(engine)
