"""This module provides a base class for activity metrics."""

import typing
from abc import abstractmethod
from typing import TYPE_CHECKING

import pint
from pint import UndefinedUnitError

if TYPE_CHECKING:
    from sports_planner_lib.db.schemas import Activity, Athlete

ureg = pint.UnitRegistry()


class Metric:
    """Activity metric base class.

    Parameters
    ----------
    activity
        The activity to calculate the metric for.
    results
        Any metrics previously calculated.
    """

    #: The name of the metric
    name: str
    #: The description of the metric
    description: str
    #: The value of the metric
    value: float
    #: Any metrics depended on by the metric
    deps: list = []
    #: Any metrics that should be calculated before the metric
    weak_deps: list = []
    #: The format string for the metric
    format_string = ""
    #: The unit for the metric
    unit = ""
    #: When the method of calculating the metric was last changed
    last_changed = None
    #: The data type of the metric
    data_type = None
    #: How to aggregate the metric, e.g. sum, mean, min, max
    aggregation_function = ""
    #: Whether the metric can be cached in the db
    cache = True

    needed_columns = []

    def __init__(self, activity: "Activity", athlete: "Athlete"):
        self.activity = activity
        self.athlete = athlete

    @abstractmethod
    def compute(self):
        """Compute the value of the metric."""

    def get_metric(self, metric: type["Metric"]) -> typing.Any:
        """Get the value of a previously calculated metric.

        Parameters
        ----------
        metric
            The metric to retrieve

        Returns
        -------
        typing.Any
            The value of the metric retrieved
        """
        assert metric in self.deps or metric in self.weak_deps
        return self.activity.get_metric(metric, athlete=self.athlete)

    def get_applicable(self):
        """Get whether this metric is applicable.

        Returns
        -------
        bool
            `True` if the metric is applicable otherwise `False`
        """
        rtn = self.applicable()
        # for dep in self.deps:
        #     rtn = rtn and dep(self.activity).get_applicable()
        return rtn

    def _has_needed_columns(self):
        rtn = all(
            [
                column in self.activity.available_columns
                for column in self.needed_columns
            ]
        )
        return rtn

    @abstractmethod
    def _applicable(self):
        return True

    def applicable(self):
        """Is this metric applicable.

        Returns
        -------
        bool
        """
        return self._has_needed_columns() and self._applicable()

    def add_dep(self, dep: type["Metric"]):
        """Add a dependency to this metric.

        Parameters
        ----------
        dep
            The metric to add as a dependency
        """
        self.deps.append(dep)

    @classmethod
    def _get_target_unit(cls, sport=None):
        metric_unit_map = {"m/s": "km/h", "s/m": "min/km"}

        if sport == "running" and cls.unit == "m/s":
            pass

        if cls.unit in metric_unit_map:
            return metric_unit_map[cls.unit]
        return cls.unit

    @classmethod
    def _do_format(cls, value, target_unit=None):
        """Converts this to a string.

        Returns
        -------
        str
        """
        if hasattr(cls, "_format"):
            return cls._format(value)
        try:
            unit = ureg.parse_units(cls.unit)
        except UndefinedUnitError:
            return f"{value:{cls.format_string}}"
        if target_unit is None:
            target_unit = cls.unit
        return f"{(value * unit).to(target_unit).m:{cls.format_string}}"

    @classmethod
    def format(cls, value, sport=None):
        target_unit = cls._get_target_unit(sport)
        return cls.name, cls._do_format(value, target_unit), target_unit


class ActivityMetric(Metric):
    """Metric computed for a specific activity."""
