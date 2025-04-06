from sports_planner_lib.metrics.activity import (
    AverageHR,
    AveragePower,
    CyclingMetric,
    TimerTime,
)
from sports_planner_lib.metrics.athlete import FTP


class CogganNP(CyclingMetric):
    name = "Normalized power"

    format_string = ".0f"

    needed_columns = ["power"]

    def compute(self):
        power30 = self.activity.records_df["power"].rolling(window="30s").mean()

        power30_4 = power30**4

        return power30_4.mean() ** 0.25


class CogganVI(CyclingMetric):
    name = "Variability index"

    deps = CyclingMetric.deps + [CogganNP, AveragePower]

    def compute(self):
        np = self.get_metric(CogganNP)
        ap = self.get_metric(AveragePower)

        return np / ap


class CogganIF(CyclingMetric):
    name = "Intensity factor"

    deps = CyclingMetric.deps + [CogganNP, FTP]

    format_string = ".2f"

    def compute(self):
        np = self.get_metric(CogganNP)
        ftp = self.get_metric(FTP)

        return np / ftp


class CogganTSS(CyclingMetric):
    name = "Training stress score"

    deps = CyclingMetric.deps + [CogganNP, FTP, TimerTime]

    format_string = ".1f"

    aggregation_function = "sum"

    def compute(self):
        np = self.get_metric(CogganNP)
        ftp = self.get_metric(FTP)
        ttt = self.get_metric(TimerTime)

        raw = np * np / ftp * ttt

        normalizing_factor = ftp * 3600

        return raw / normalizing_factor * 100


class CogganEF(CyclingMetric):
    name = "Efficiency factor (Coggan NP based)"

    deps = CyclingMetric.deps + [CogganNP, AverageHR]

    def compute(self):
        np = self.get_metric(CogganNP)
        ah = self.get_metric(AverageHR)

        return np / ah
