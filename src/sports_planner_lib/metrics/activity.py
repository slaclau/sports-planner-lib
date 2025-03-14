"""This module provides generic metrics applicable to a range of activities."""

import datetime
import typing
from abc import ABC

import numpy as np
import pandas as pd
import sweat  # type: ignore

from sports_planner_lib.metrics.base import ActivityMetric
from sports_planner_lib.utils import format  # pylint: disable=W0622


class ActivityDate(ActivityMetric):
    """The total time during which the timer is running during an activity."""

    name = "Date"

    def applicable(self) -> bool:
        """

        Returns
        -------
        bool
            Always `True`
        """
        return True

    def compute(self) -> int:
        """
        Returns
        -------
        int
            The number of seconds
        """
        try:
            ts = self.activity.timestamp
            return datetime.datetime.fromtimestamp(ts.total_seconds()).date()
        except AttributeError:
            pass
        except TypeError:
            pass
        return self.activity.records_df.index[0].date()


class TimerTime(ActivityMetric):
    """The total time during which the timer is running during an activity."""

    name = "Total time"

    def applicable(self) -> bool:
        """

        Returns
        -------
        bool
            Always `True`
        """
        return True

    def compute(self) -> int:
        """
        Returns
        -------
        int
            The number of seconds
        """
        try:
            return typing.cast(int, self.activity.total_timer_time)
        except AttributeError:
            pass
        except TypeError:
            pass
        return typing.cast(
            int, self.activity.records_df.index[-1] - self.activity.records_df.index[0]
        ).total_seconds()

    @classmethod
    def _format(cls, value):
        return format.time(value)


class ElapsedTime(ActivityMetric):
    """The total time between the start and end of an activity."""

    name = "Elapsed time"

    def applicable(self) -> bool:
        """

        Returns
        -------
        bool
            Always `True`
        """
        return True

    def compute(self) -> int:
        """
        Returns
        -------
        int
            The number of seconds
        """
        return typing.cast(
            int, self.activity.records_df.index[-1] - self.activity.records_df.index[0]
        ).total_seconds()

    @classmethod
    def _format(cls, value):
        return format.time(value)


class MovingTime(ActivityMetric):
    """The total moving time in an activity."""

    name = "Moving time"

    def applicable(self) -> bool:
        """

        Returns
        -------
        bool
            Always `True`
        """
        return True

    def compute(self) -> int:
        """
        Returns
        -------
        int
            The number of seconds
        """
        return self.activity.records_df.speed[
            self.activity.records_df.speed > 0
        ].count()

    @classmethod
    def _format(cls, value):
        return format.time(value)


class TotalAscent(ActivityMetric):
    """The total ascent in an activity."""

    name = "Ascent"
    unit = "m"

    format_string = ".0f"

    def applicable(self) -> bool:
        """

        Returns
        -------
        bool
            Always `True`
        """
        return True

    def compute(self):
        """
        Returns
        -------
        int
            The elevation gain in metres
        """
        first = True
        hysteresis = 3
        ascent = 0
        for point in self.activity.records_df.altitude:
            if first:
                previous = point
                first = False
            if point > previous + hysteresis:
                ascent += point - previous
                previous = point
            elif point < previous - hysteresis:
                previous = point

        return ascent


class TotalDescent(ActivityMetric):
    """The total descent in an activity."""

    name = "Descent"
    unit = "m"

    format_string = ".0f"

    def applicable(self) -> bool:
        """

        Returns
        -------
        bool
            Always `True`
        """
        return True

    def compute(self):
        """
        Returns
        -------
        int
            The elevation loss in metres
        """
        first = True
        hysteresis = 3
        descent = 0
        for point in self.activity.records_df.altitude:
            if first:
                previous = point
                first = False
            if point < previous - hysteresis:
                descent += previous - point
                previous = point
            elif point > previous + hysteresis:
                previous = point

        return descent


class TotalDistance(ActivityMetric):
    """The total distance travelled in an activity."""

    name = "Total distance"
    unit = "km"

    def applicable(self) -> bool:
        """

        Returns
        -------
        bool
            Always `True`
        """
        return True

    def compute(self) -> int:
        """
        Returns
        -------
        int
            The distance in metres
        """
        return self.activity.records_df.distance.iloc[-1]

    @classmethod
    def _format(cls, value):
        return f"{value/1000:0.2f}"


class Sport(ActivityMetric):
    """The type of activity, including fields for sport, subsport, and name."""

    name = "Sport"

    def applicable(self):
        """
        Returns
        -------
        bool
            `True` if the activity summary contains the key "sport" or the "sport" field
            of the dataframe has a unique value.
        """
        return True

    def compute(self):
        """
        Returns
        -------

        dict[str, str]
            A dictionary with the following possible keys: sport, sub_sport, and name.
            sport is always included.
        """
        if len(self.activity.sessions) == 1:
            self.activity.sessions[0].sport
            return {
                "sport": self.activity.sessions[0].sport,
                "sub_sport": self.activity.sessions[0].sub_sport,
            }
        return {"sport": "UNKNOWN"}

    @classmethod
    def _format(cls, value):
        if "name" in value:
            return value["name"].title()
        else:
            return value["sport"].title()


class AverageSpeed(ActivityMetric):
    """The average speed over an activity."""

    name = "Average speed"
    unit = "m/s"
    format_string = ".2f"

    deps = [TimerTime]

    def applicable(self):
        """

        Returns
        -------
        bool
            `True` if the dataframe contains a "speed" column
        """
        if "speed" in self.df.columns:
            return True
        return False

    def compute(self) -> float:
        """

        Returns
        -------
        float
            The average speed (total distance divided by :class:`TimerTime`
        """
        time = self.get_metric(TimerTime)
        return self.df["distance"].iloc[-1] / time


class AveragePower(ActivityMetric):
    """The average power over an activity."""

    name = "Average power"
    unit = "W"
    format_string = ".0f"

    def applicable(self):
        """

        Returns
        -------
        bool
            `True` if the dataframe contains a "power" column
        """
        if "power" in self.df.columns:
            return True
        return False

    def compute(self):
        """

        Returns
        -------
        float
            The average power
        """
        return self.df["power"].replace(0, np.nan).mean(skipna=True)


class AverageHR(ActivityMetric):
    """The average heartrate over an activity."""

    name = "Average heart rate"
    unit = "bpm"
    format_string = ".0f"

    def applicable(self):
        """

        Returns
        -------
        bool
            `True` if the dataframe contains a "power" column
        """
        if "heartrate" in self.df.columns:
            return True
        return False

    def compute(self):
        """

        Returns
        -------
        float
            The average heartrate
        """
        return self.df["heartrate"].mean()


class ThresholdHeartrate(ActivityMetric):
    name = "Lactate threshold heart rate"
    unit = "bpm"
    format_string = ".0f"
    deps = [Sport]

    def compute(self):
        sport = self.get_metric(Sport)["sport"]
        if sport == "running":
            return 175
        if sport == "cycling":
            return 177
        return 175


class DurationRegressor(sweat.PowerDurationRegressor):
    def __init__(
        self,
        model="2 param",
        cp=300,
        w_prime=20000,
        p_max=1000,
        tau=300,
        tau2=1800,
        tcp_max=1800,
        a=50,
    ):
        self.model = model
        self.cp = cp
        self.w_prime = w_prime
        self.p_max = p_max
        self.tau = tau
        self.tau2 = tau2
        self.tcp_max = tcp_max
        self.a = a

    def _aerobic(self, X, cp, w_prime, tau, tau2, a):
        t = X.T[0]
        result = cp * (1 - np.exp(-t / tau2))

        return np.where(
            t <= self.tcp_max, result, result - a * np.log(t / self.tcp_max)
        )

    def _anaerobic(self, X, cp, w_prime, tau, tau2, a):
        t = X.T[0]
        return w_prime / t * (1 - np.exp(-t / tau))

    def predict_ae(self, X):
        assert self.model == "pt"
        _, params = self._model_selection()
        func = self._aerobic

        args = []
        for param_name in params:
            args.append(getattr(self, f"{param_name}_"))

        return func(X, *args)

    def predict_an(self, X):
        assert self.model == "pt"
        _, params = self._model_selection()
        func = self._anaerobic

        args = []
        for param_name in params:
            args.append(getattr(self, f"{param_name}_"))

        return func(X, *args)

    def _pt_model(self, X, cp, w_prime, tau, tau2, a):
        return self._aerobic(X, cp, w_prime, tau, tau2, a) + self._anaerobic(
            X, cp, w_prime, tau, tau2, a
        )

    def _model_selection(self):
        if self.model == "pt":
            func = self._pt_model
            params = ["cp", "w_prime", "tau", "tau2", "a"]
            return func, params
        return super()._model_selection()


class RunningMetric(ActivityMetric, ABC):
    """A base class for running specific metrics."""

    deps = [Sport]

    def applicable(self) -> bool:
        """

        Returns
        -------
        bool
            `True` if the activity is a run
        """
        try:
            sport = self.get_metric(Sport)["sport"]
        except KeyError:
            return False
        if sport == "running":
            return True
        return False


class CyclingMetric(ActivityMetric, ABC):
    """A base class for cycling specific metrics."""

    deps = [Sport]

    def applicable(self) -> bool:
        """

        Returns
        -------
        bool
            `True` if the activity is a cycle
        """
        sport = self.get_metric(Sport)["sport"]
        if sport == "cycling":
            return True
        return False
