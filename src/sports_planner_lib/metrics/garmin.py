from abc import ABC

import numpy as np

from sports_planner_lib.metrics.activity import CyclingMetric, RunningMetric
from sports_planner_lib.metrics.base import ActivityMetric


class Firstbeat(ActivityMetric, ABC):
    field_name: str
    scale: float
    allow_zero: bool

    def applicable(self):
        for unknown in self.activity.unknown_messages:
            if unknown.type == "firstbeat":
                return True
        return False

    def compute(self):
        for unknown in self.activity.unknown_messages:
            if unknown.type == "firstbeat":
                rtn = unknown.record[self.field_name] * self.scale
                if rtn == 0 and not self.allow_zero:
                    rtn = np.nan
                return rtn


class VO2Max(Firstbeat):
    name = "VO2Max (Garmin)"
    field_name = "unknown_7"
    scale = 3.5 / 65536
    allow_zero = False
    unit = "mL/kg/min"
    format_string = ".1f"


class RunningVO2Max(RunningMetric, VO2Max):
    name = "Running VO2Max (Garmin)"
    deps = RunningMetric.deps + [VO2Max]


class CyclingVO2Max(CyclingMetric, VO2Max):
    name = "Cycling VO2Max (Garmin)"
    deps = CyclingMetric.deps + [VO2Max]
