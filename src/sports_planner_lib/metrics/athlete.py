from sports_planner_lib.metrics.base import Metric


class AthleteMetric(Metric):
    """Metric describing property of the athlete."""

    def applicable(self):
        return True


class Height(AthleteMetric):
    name = "Height"

    def compute(self):
        return 1.83


class Weight(AthleteMetric):
    name = "Weight"

    def compute(self):
        return 73
