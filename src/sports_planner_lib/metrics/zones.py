from typing import cast

import numpy as np
import pandas as pd

from sports_planner_lib.metrics.activity import (
    ActivityMetric,
    Sport,
)
from sports_planner_lib.metrics.athlete import LactateThresholdHR
from sports_planner_lib.metrics.athlete import FTP


class ZoneDefinitionsMeta(type):
    """A metaclass for :class:`ZoneDefinitions`."""

    classes = {}

    @classmethod
    def __getitem__(mcs, item):
        """Return a new subclass of :class:`ZoneDefinitions`."""
        if item in mcs.classes:
            return mcs.classes[item]
        column = item
        deps = [Sport]
        if column == "heartrate":
            deps.append(LactateThresholdHR)
        elif column == "power":
            deps.append(FTP)

        zone_defs = type(
            f'ZoneDefinitions["{column}"]',
            (ZoneDefinitions,),
            {
                "column": column,
                "deps": deps,
                "name": f"{column} zone definitions",
            },
        )
        mcs.classes[item] = zone_defs
        return zone_defs


class ZoneDefinitions(ActivityMetric, metaclass=ZoneDefinitionsMeta):
    """The zone definitions for a specific column."""

    column: str
    needed_columns = [column]

    def compute(self) -> float:
        """

        Returns
        -------
        tuple[list[float], list[str]]
            A list of zone boundaries and a list of zone labels.
        """
        mode = "fixed"
        sport = self.get_metric(Sport)
        base = None
        if self.column == "heartrate":
            mode = "percent"
            base = 175
            bins = [0, 65, 80, 89, 95, 100, 114]
            labels = ["Z0", "Z1", "Z2", "Z3", "Z4", "Z5"]
        elif self.column == "power":
            mode = "percent"
            base = self.get_metric(CyclingFTP)
            bins = [0, 54, 74, 89, 103, 128, 157]
            labels = ["Z1", "Z2", "Z3", "Z4", "Z5", "Z6"]

        if mode == "percent":
            bins = np.array(bins) * base / 100

        assert len(labels) == len(bins) - 1
        return bins, labels


class ZonesMeta(type):
    """A metaclass for :class:`Zones`."""

    classes = {}

    @classmethod
    def __getitem__(mcs, item):
        """Return a new subclass of :class:`Zones`."""
        if item in mcs.classes:
            return mcs.classes[item]
        column = item
        zones = type(
            f'Zones["{column}"]',
            (Zones,),
            {
                "column": column,
                "deps": [ZoneDefinitions[column]],
                "name": f"times in {column} zones",
            },
        )
        mcs.classes[item] = zones
        return zones


class Zones(ActivityMetric, metaclass=ZonesMeta):
    """The zones for a specific column."""

    column: str

    needed_columns = [column]

    def compute(self) -> pd.DataFrame:
        """

        Returns
        -------
        pd.DataFrame
            The zones
        """
        zone_defs = self.get_metric(ZoneDefinitions[self.column])
        bins = zone_defs[0]
        labels = zone_defs[1]
        return self.activity.records_df[self.column].sweat.time_in_zone(
            bins=bins, labels=labels
        )


class TimeInZoneMeta(type):
    """A metaclass for :class:`TimeInZone`."""

    classes = {}

    @classmethod
    def __getitem__(mcs, item):
        """Return a new subclass of :class:`TimeInZone`."""
        if item in mcs.classes:
            return mcs.classes[item]
        column = item[0]
        zone = item[1]
        time_in_zone = type(
            f'TimeInZone["{column}", {zone}]',
            (TimeInZone,),
            {
                "column": column,
                "zone": zone,
                "deps": [Zones[column]],
                "name": f"Time in {column} zone {zone}",
            },
        )
        mcs.classes[item] = time_in_zone
        return time_in_zone


class TimeInZone(ActivityMetric, metaclass=TimeInZoneMeta):
    """The maximum of a quantity for a given time."""

    column: str
    zone: str
    unit = "td"

    def applicable(self) -> bool:
        """

        Returns
        -------
        bool
            Always `True`
        """
        return True

    def compute(self) -> float:
        """

        Returns
        -------
        float
            The time in the relevant zone
        """
        zones = self.get_metric(Zones[self.column])
        return cast(float, zones[self.zone])
