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
