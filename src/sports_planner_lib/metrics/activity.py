"""This module provides generic metrics applicable to a range of activities."""

import datetime
import typing
from abc import ABC

import numpy as np
import pandas as pd
import sweat  # type: ignore

from sports_planner_lib.metrics.base import ActivityMetric
from sports_planner_lib.utils import format  # pylint: disable=W0622


class TimerTime(ActivityMetric):
    """The total time during which the timer is running during an activity."""

    name = "Total timer time"
    unit = "s"

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
            return typing.cast(int, self.activity.details["total_timer_time"])
        except AttributeError:
            pass
        except TypeError:
            pass
        return typing.cast(
            int, self.activity.records_df.index[-1] - self.activity.records_df.index[0]
        ).total_seconds()


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
        return len(self.activity.sessions) == 1

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


class AverageSpeed(ActivityMetric):
    """The average speed over an activity."""

    name = "Average speed"
    unit = "m/s"
    format = ".2f"

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
        return self.df["distance"][-1] / time


class AveragePower(ActivityMetric):
    """The average power over an activity."""

    name = "Average power"
    unit = "W"
    format = ".0f"

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
    format = ".0f"

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
    format = ".0f"
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


class CurveMeta(type):
    """A metaclass for :class:`Curve`."""

    classes = {}

    @classmethod
    def __getitem__(mcs, item):
        """Return a new subclass of :class:`Curve`."""
        if item in mcs.classes:
            return mcs.classes[item]
        curve = type(
            f'Curve["{item}"]',
            (Curve,),
            {"column": item, "name": f"{item}-duration curve"},
        )
        mcs.classes[item] = curve
        return curve


class Curve(ActivityMetric, metaclass=CurveMeta):
    """A curve showing the maximum durations for different values."""

    column: str  #: The column of the dataframe to compute the curve for

    def applicable(self):
        """

        Returns
        -------
        bool
            `True` if the column exists
        """
        return self.column in self.df.columns

    def compute(self):
        """

        Returns
        -------
        dict
            A dictionary with the following keys: x, y, and predictions.
            x and y are the actual calculated values, predictions is a dataframe with
            columns for different predictions.
        """
        # data = self.meanmaxes_df[]
        # x = pc_df.index.total_seconds()
        # X = sweat.array_1d_to_2d(x)
        # y = pc_df["mean_max_" + self.column]
        # data_dict = {}
        #
        # try:
        #     two_param = DurationRegressor(model="2 param")
        #     two_param.fit(X, y)
        #     data_dict["2 param"] = two_param.predict(X)
        # except RuntimeError as e:
        #     pass
        # except ValueError as e:
        #     pass
        #
        # try:
        #     three_param = DurationRegressor(model="3 param")
        #     three_param.fit(X, y)
        #     data_dict["3 param"] = three_param.predict(X)
        # except RuntimeError as e:
        #     pass
        # except ValueError as e:
        #     pass
        #
        # try:
        #     exponential = DurationRegressor(model="exponential")
        #     exponential.fit(X, y)
        #     data_dict["exponential"] = exponential.predict(X)
        # except RuntimeError as e:
        #     pass
        # except ValueError as e:
        #     pass
        #
        # try:
        #     omni = DurationRegressor(model="omni")
        #     omni.fit(X, y)
        #     data_dict["omni"] = omni.predict(X)
        # except RuntimeError as e:
        #     pass
        # except ValueError as e:
        #     pass
        #
        # try:
        #     pt = DurationRegressor(model="pt")
        #     pt.fit(X, y)
        #     data_dict["pt"] = pt.predict(X)
        #     data_dict["ae"] = pt.predict_ae(X)
        #     data_dict["an"] = pt.predict_an(X)
        # except RuntimeError as e:
        #     pass
        # except ValueError as e:
        #     pass
        #
        # data = pd.DataFrame(data_dict)
        #
        # return {"x": x, "y": y, "predictions": data}


class MeanMaxMeta(type):
    """A metaclass for :class:`MeanMax`."""

    classes = {}

    @classmethod
    def __getitem__(mcs, item):
        """Return a new subclass of :class:`MeanMax`."""
        if item in mcs.classes:
            return mcs.classes[item]
        column = item[0]
        time = item[1]
        duration = format.time(time)
        mean_max = type(
            f'MeanMax["{column}", {time}]',
            (MeanMax,),
            {
                "column": column,
                "time": time,
                "deps": [],
                "name": f"{duration} max {column}",
            },
        )
        mcs.classes[item] = mean_max
        return mean_max


class MeanMax(ActivityMetric, metaclass=MeanMaxMeta):
    """The maximum of a quantity for a given time."""

    column: str
    time: int
    format = ".2f"

    def applicable(self) -> bool:
        """

        Returns
        -------
        bool
            `True` if the relevant :class:`Curve` extends for a sufficient duration
        """
        return self.column in self.df.columns and len(self.df.index) >= self.time

    def compute(self) -> float:
        """

        Returns
        -------
        float
            The corresponding meanmax value
        """
        return float(
            getattr(self.activity.meanmaxes[self.time - 1], f"mean_max_{self.column}")
        )


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
