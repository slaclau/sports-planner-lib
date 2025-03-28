from sqlalchemy import text
from sqlalchemy.orm import Session

from sports_planner_lib.db.other import ConfiguredValue
from sports_planner_lib.metrics.activity import Sport
from sports_planner_lib.metrics.base import Metric


class ConfiguredValueMetric(Metric):
    """Metric describing property of the athlete. Stored as :class:`ConfiguredValue`."""

    deps = [Sport]

    field_name: str
    field_scale: float = 1
    cache = False

    def compute(self):
        sport = self.get_metric(Sport)["sport"]
        with self.athlete.Session() as session:
            vals = (
                session.query(ConfiguredValue.value)
                .filter(ConfiguredValue.name == self.field_name)
                .filter(ConfiguredValue.sport == sport)
                .filter(ConfiguredValue.date <= self.activity.timestamp.date())
                .order_by(ConfiguredValue.date.desc())
                .first()
            )
            if vals:
                return self.field_scale * vals[0]

    @property
    def validity_date(self):
        sport = self.get_metric(Sport)["sport"]
        with self.athlete.Session() as session:
            return (
                session.query(ConfiguredValue)
                .filter(ConfiguredValue.name == self.field_name)
                .filter(ConfiguredValue.sport == sport)
                .filter(ConfiguredValue.date <= self.activity.timestamp.date())
                .order_by(ConfiguredValue.date.desc())
                .first()
                .versions.all()[-1]
                .transaction.issued_at
            )


class FTP(ConfiguredValueMetric):
    name = "Functional Threshold Power"

    field_name = "ftp"


class LactateThresholdHR(ConfiguredValueMetric):
    name = "Lactate Threshold Heartrate"

    field_name = "lthr"


class Height(ConfiguredValueMetric):
    name = "Height"

    def compute(self):
        return 1.83


class Weight(ConfiguredValueMetric):
    name = "Weight"

    def compute(self):
        return 73
