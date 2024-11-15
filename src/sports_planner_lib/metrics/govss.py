import numpy as np

from sports_planner_lib.metrics.activity import RunningMetric, TimerTime
from sports_planner_lib.metrics.athlete import Height, Weight


def calculate_power(weight, height, speed, slope=0.0, distance=0.0, initial_speed=0.0):
    Af = (0.2025 * (height**0.725) * (weight**0.425)) * 0.266
    cAero = 0.5 * 1.2 * 0.9 * Af * speed * speed / weight

    cKin = (
        0.5 * (speed * speed - initial_speed * initial_speed) / distance
        if distance
        else 0
    )

    cSlope = (
        155.4 * slope**5
        - 30.4 * slope**4
        - 43.3 * slope**3
        + 46.3 * slope**2
        + 19.5 * slope
        + 3.6
    )

    eff = (0.25 + 0.054 * speed) * (1 - 0.5 * speed / 8.33)

    return (cAero + cKin + cSlope * eff) * speed * weight


class LNP(RunningMetric):
    name = "Lactate normalized power"
    unit = "W"
    format = ".0f"

    deps = RunningMetric.deps + [Height, Weight]

    def compute(self):
        weight = self.get_metric(Weight)
        height = self.get_metric(Height)

        if "altitude" not in self.df.columns:
            self.df["altitude"] = 0

        self.df["distance_diff"] = self.df["distance"].diff()
        mask = self.df["distance_diff"] < 0.1
        self.df.loc[mask, "distance_diff"] = 0

        self.df["slope"] = self.df["altitude"].diff() / self.df["distance_diff"]
        self.df["slope"].replace([np.inf, -np.inf], 0, inplace=True)

        self.df["d_speed"] = self.df["distance"].diff()

        rolling_df = self.df.rolling(window="120s", method="table")

        df = self.df.copy()

        df["speed120"] = (
            self.df["d_speed"].rolling(window="120s").mean()
        )  # engine="numba")
        df["slope120"] = (
            self.df["slope"].rolling(window="120s").mean()
        )  # engine="numba")

        df["distance120"] = self.df["distance"].shift(120) - self.df["distance"]

        df["begin_speed"] = self.df["d_speed"].shift(120)

        def get_power(row):
            return calculate_power(
                weight,
                height,
                row[0],
                row[1],
                row[2],
                row[3],
            )

        self.df["power"] = df[
            ["speed120", "slope120", "distance120", "begin_speed"]
        ].apply(get_power, axis=1, raw=True)

        self.df["power30"] = self.df["power"].rolling(window="30s").mean()

        power30_4 = self.df["power30"] ** 4

        rtn = power30_4.mean() ** 0.25
        if np.isnan(rtn):
            return 0
        return rtn


class XPace(RunningMetric):
    name = "xPace"
    unit = "m/s"
    format = ".2f"

    deps = RunningMetric.deps + [Height, Weight, LNP]

    def compute(self):
        weight = self.get_metric(Weight)
        height = self.get_metric(Height)

        lnp = self.get_metric(LNP)

        low = 0
        high = 10
        error = 1

        if lnp <= 0:
            speed = low
        elif lnp >= calculate_power(weight, height, high):
            speed = high
        else:
            while True:
                speed = (low + high) / 2
                watts = calculate_power(weight, height, speed)
                if abs(watts - lnp) < error:
                    break
                elif watts < lnp:
                    low = speed
                elif watts > lnp:
                    high = speed

        return speed


class CV(RunningMetric):
    name = "Critical velocity"
    unit = "m/s"
    format = ".2f"

    def compute(self):
        return 3.3333


class RTP(RunningMetric):
    name = "Running threshold power"
    unit = "W"
    format = ".2f"

    deps = RunningMetric.deps + [Height, Weight, CV]

    def compute(self):
        weight = self.get_metric(Weight)
        height = self.get_metric(Height)

        cv = self.get_metric(CV)

        return calculate_power(weight, height, cv)


class IWF(RunningMetric):
    name = "Intensity weighting factor"
    format = ".2f"

    deps = RunningMetric.deps + [LNP, RTP]

    def compute(self):
        lnp = self.get_metric(LNP)
        rtp = self.get_metric(RTP)

        return lnp / rtp


class GOVSS(RunningMetric):
    name = "GOVSS"
    format = ".1f"

    deps = RunningMetric.deps + [LNP, RTP, TimerTime]

    def compute(self):
        lnp = self.get_metric(LNP)
        rtp = self.get_metric(RTP)

        time = self.get_metric(TimerTime)

        raw = lnp * lnp / rtp * time

        normalizing_factor = rtp * 3600

        return raw / normalizing_factor * 100
