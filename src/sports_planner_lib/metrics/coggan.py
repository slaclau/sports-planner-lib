from sports_planner_lib.metrics.activity import (
    AverageHR,
    AveragePower,
    CyclingMetric,
    TimerTime,
)


class CogganNP(CyclingMetric):
    name = "Normalized power"

    format = ".0f"

    def compute(self):
        self.df["power30"] = self.df["power"].rolling(window="30s").mean()

        power30_4 = self.df["power30"] ** 4

        return power30_4.mean() ** 0.25


class CyclingFTP(CyclingMetric):
    name = "Functional threshold power (cycling)"

    def compute(self):
        return 206


class CogganVI(CyclingMetric):
    name = "Variability index"

    deps = CyclingMetric.deps + [CogganNP, AveragePower]

    def compute(self):
        np = self.get_metric(CogganNP)
        ap = self.get_metric(AveragePower)

        return np / ap


class CogganIF(CyclingMetric):
    name = "Intensity factor"

    deps = CyclingMetric.deps + [CogganNP, CyclingFTP]

    format = ".2f"

    def compute(self):
        np = self.get_metric(CogganNP)
        ftp = self.get_metric(CyclingFTP)

        return np / ftp


class CogganTSS(CyclingMetric):
    name = "Training stress score"

    deps = CyclingMetric.deps + [CogganNP, CyclingFTP, TimerTime]

    format = ".1f"

    def compute(self):
        np = self.get_metric(CogganNP)
        ftp = self.get_metric(CyclingFTP)
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
