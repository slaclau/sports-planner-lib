"""This module provides a base class for activity metrics."""

import typing
from abc import abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sports_planner_lib.db.schemas import Activity, Athlete


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
    #: The format string for the metric
    format = ""
    #: The unit for the metric
    unit = ""
    #: When the method of calculating the metric was last changed
    last_changed = None
    #: The data type of the metric
    data_type = None

    def __init__(self, activity: "Activity", results=None):
        self.activity = activity
        self.df = self.activity.records_df
        self.results = results

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
        assert metric in self.deps
        from sports_planner_lib.metrics.activity import (  # pylint: disable=C0415
            CurveMeta,
            MeanMaxMeta,
        )
        from sports_planner_lib.metrics.zones import (
            TimeInZoneMeta,
            ZoneDefinitionsMeta,
            ZonesMeta,
        )

        if metric.__class__ not in [
            CurveMeta,
            MeanMaxMeta,
            TimeInZoneMeta,
            ZoneDefinitionsMeta,
            ZonesMeta,
        ]:
            return self.results[metric]
        return self.results[metric.name]

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

    @abstractmethod
    def applicable(self):
        """Is this metric applicable.

        Returns
        -------
        bool
        """

    def add_dep(self, dep: type["Metric"]):
        """Add a dependency to this metric.

        Parameters
        ----------
        dep
            The metric to add as a dependency
        """
        self.deps.append(dep)


class ActivityMetric(Metric):
    """Metric computed for a specific activity."""

    @abstractmethod
    def applicable(self):
        pass

    @abstractmethod
    def compute(self):
        pass
