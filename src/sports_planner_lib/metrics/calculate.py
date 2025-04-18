import logging
from graphlib import TopologicalSorter
from time import time

import pyparsing as pp

from sports_planner_lib.metrics import *
from sports_planner_lib.metrics.pdm import Curve, MeanMax

# from sports_planner_lib.metrics.zones import TimeInZone, ZoneDefinitions, Zones
from sports_planner_lib.utils.logging import debug_time, info_time, logtime

all_metrics = None
metrics_map = None

logger = logging.getLogger(__name__)


def get_all_metrics() -> set[type[base.Metric]]:
    global all_metrics
    if all_metrics is None:
        all_metrics = _get_all_subclasses(base.Metric)
    return all_metrics


def get_metrics_map():
    global metrics_map
    if metrics_map is None:
        metrics_map = {metric.__name__: metric for metric in get_all_metrics()}
    return metrics_map


def get_metric(metric_name):
    metrics_map = get_metrics_map()
    if metric_name in metrics_map:
        return metrics_map[metric_name]
    return eval(metric_name)


def _get_all_subclasses(cls) -> set[type]:
    return set(cls.__subclasses__()).union(
        [s for c in cls.__subclasses__() for s in _get_all_subclasses(c)]
    )


metric_name_grammar = (
    pp.Word(pp.alphanums).set_name("class")
    + pp.Opt(
        "["
        + pp.Opt(
            pp.Group(
                pp.delimited_list(
                    pp.python_quoted_string.add_parse_action(pp.remove_quotes)
                    | pp.common.real
                    | pp.common.integer,
                    ",",
                ).set_name("args")
            )
        )
        + "]"
    )
    + pp.Opt(
        "["
        + pp.Opt(
            pp.Group(
                pp.delimited_list(
                    pp.python_quoted_string | pp.common.real | pp.common.integer,
                    ",",
                ).set_name("fields")
            )
        )
        + "]"
    )
)


def parse_metric_string(name):
    if metric_name_grammar.matches(name):
        res = metric_name_grammar.parse_string(name)
        class_name = res[0]

        metric = get_metrics_map()[class_name]
        fields = []
        if len(res) >= 4:
            args = [arg for arg in res[2]]
            try:
                if len(args) == 1 and args[0] == "":
                    pass
                elif len(args) == 1:
                    metric = metric[args[0]]
                else:
                    metric = metric[*args]
                if len(res) == 7:
                    fields = [arg for arg in res[5]]
            except TypeError:
                fields = args
        return metric, fields
    return None, []


logger.debug(f"Getting all metrics {get_all_metrics()}")


class MetricsCalculator:
    def __init__(
        self,
        activity: "Activity",
        desired_metrics,
        pre_ordered=False,
    ):
        self.activity = activity
        self.deps = desired_metrics if pre_ordered else self.order_deps(desired_metrics)
        self.metrics = activity.metrics
        self.compute()

    @staticmethod
    def order_deps(desired_metrics):
        required_metrics = set()
        i = 0

        while i < len(desired_metrics):
            metric = desired_metrics[i]
            required_metrics.add(metric)
            for _metric in metric.deps + metric.weak_deps:
                if _metric not in desired_metrics:
                    desired_metrics.append(_metric)
            i += 1

        deps_dict = {
            metric: metric.deps + metric.weak_deps for metric in required_metrics
        }
        deps = list(TopologicalSorter(deps_dict).static_order())
        return deps

    @debug_time
    def compute(self, recompute_all=False):
        # recompute_all = True
        computed = []
        retrieved = [metric for metric in self.metrics]
        recompute = []
        debug_string = ""
        try:
            debug_string += self.activity.name + "\n"
        except TypeError:
            pass
        for metric in self.deps:
            try:
                metric_instance = metric(self.activity, self.metrics)
                if metric_instance.get_applicable() and (
                    metric.name not in retrieved or metric in recompute or recompute_all
                ):
                    self.activity.add_metric(metric.name, metric_instance.compute())
                    computed.append(metric.name)
            except AssertionError as e:
                print(f"AssertionError: {e}")
                print(metric.name)
        debug_string += f"Retrieved {retrieved} from cache\n"
        debug_string += f"Computed and cached {computed}\n"
        logger.debug(debug_string)

    def __str__(self):
        string = f"{self.__class__.__name__}\n"
        for metric in self.metrics:
            string += f"{metric.name}: {self.metrics[metric]}\n"

        return string


if __name__ == "__main__":
    start = time()
    print(get_metrics_map())
    e1 = time()
    print(e1 - start)
    print(get_metrics_map())
    print(time() - e1)
