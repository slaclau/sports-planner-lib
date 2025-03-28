from abc import ABC

import numpy as np

from sports_planner_lib.metrics.activity import CyclingMetric, RunningMetric
from sports_planner_lib.metrics.base import ActivityMetric


class UnknownMessageMetric(ActivityMetric, ABC):
    message_name: str
    field_name: str
    scale: float
    allow_zero: bool

    def _applicable(self):
        return any(
            [
                unknown.type == self.message_name
                for unknown in self.activity.unknown_messages
            ]
        )

    def compute(self):
        for unknown in self.activity.unknown_messages:
            if unknown.type == self.message_name:
                val = unknown.record[self.field_name]
                if isinstance(val, str):
                    return val
                rtn = val * self.scale
                if rtn == 0 and not self.allow_zero:
                    rtn = np.nan
                return rtn


class Firstbeat(UnknownMessageMetric, ABC):
    message_name = "firstbeat"


class VO2Max(Firstbeat):
    name = "VO2Max (Garmin)"
    field_name = "unknown_7"
    scale = 3.5 / 65536
    allow_zero = False
    unit = "mL/kg/min"
    format_string = ".1f"
    aggregation_function = "max"


class RunningVO2Max(RunningMetric, VO2Max):
    name = "Running VO2Max (Garmin)"
    deps = RunningMetric.deps + [VO2Max]


class CyclingVO2Max(CyclingMetric, VO2Max):
    name = "Cycling VO2Max (Garmin)"
    deps = CyclingMetric.deps + [VO2Max]


class WorkoutName(UnknownMessageMetric):
    name = "Workout"
    message_name = "workout"
    field_name = "wkt_name"


class WorkoutNotes(UnknownMessageMetric):
    name = "Workout Notes"
    message_name = "workout"
    field_name = "unknown_17"
