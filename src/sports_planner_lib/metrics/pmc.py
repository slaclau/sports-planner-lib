import datetime

import numpy as np
import pandas as pd
import scipy.optimize

from sports_planner_lib.metrics.base import ActivityMetric
from sports_planner_lib.metrics.coggan import CogganTSS
from sports_planner_lib.metrics.govss import GOVSS
from sports_planner_lib.utils.logging import debug_time, info_time


class UniversalStressScore(ActivityMetric):
    name = "Universal stress score"
    format_string = ".1f"
    aggregation_function = "sum"

    weak_deps = [GOVSS, CogganTSS]

    def compute(self):
        for dep in self.weak_deps:
            try:
                return self.get_metric(dep)
            except KeyError:
                pass
        return 0


class PMC:
    @info_time
    def __init__(
        self, athlete, metric, t_short=7, t_long=42, title=None, callback_func=None
    ):
        self.athlete = athlete
        self.metric = metric
        self.t_short = t_short
        self.t_long = t_long
        self.title = title
        if callback_func is not None:
            callback_func(f"Aggregating {metric.name}")
        impulse = athlete.aggregate_metric(metric, "sum", callback_func=callback_func)
        future_impulse = athlete.aggregate_metric(
            metric, "sum", callback_func=callback_func, future=True
        )
        df = impulse.to_frame(name="impulse")
        future_impulse = future_impulse.to_frame(name="future_impulse")
        first_date = df.index[0]
        yesterday = datetime.date.today() - datetime.timedelta(days=1)
        index = pd.date_range(df.index[0], future_impulse.index[-1])
        df = df.reindex(index, fill_value=0)
        df["future_impulse"] = future_impulse["future_impulse"]

        df.loc[df.index[0], "sts"] = 0
        df.loc[df.index[0], "lts"] = 0

        exp_short = np.e ** (-1 / t_short)
        exp_long = np.e ** (-1 / t_long)

        if callback_func is not None:
            callback_func("Building PMC")
        future = False
        sts = 0
        lts = 0
        for i in df.index:
            if i.date() == first_date.date():
                continue
            impulse = df.loc[i, "impulse"]
            future_impulse = df.loc[i, "future_impulse"]
            sts = sts + (impulse - sts) * (1 - exp_short)
            lts = lts + (impulse - lts) * (1 - exp_long)
            df.loc[i, "sts"] = sts
            df.loc[i, "lts"] = lts
            df.loc[i, "predicted_sts"] = sts
            df.loc[i, "predicted_lts"] = lts

            if future:
                predicted_sts = predicted_sts + (future_impulse - predicted_sts) * (
                    1 - exp_short
                )
                predicted_lts = predicted_lts + (future_impulse - predicted_lts) * (
                    1 - exp_long
                )
                df.loc[i, "predicted_sts"] = predicted_sts
                df.loc[i, "predicted_lts"] = predicted_lts

            if i.date() == yesterday:
                predicted_sts = sts
                predicted_lts = lts
                future = True

        df["rr"] = df["lts"] - df["lts"].shift(t_short)
        df["tsb"] = df["lts"].shift(1) - df["sts"].shift(1)
        df["predicted_rr"] = df["predicted_lts"] - df["predicted_lts"].shift(t_short)
        df["predicted_tsb"] = df["predicted_lts"].shift(1) - df["predicted_sts"].shift(
            1
        )

        self.activity.records_df = df
        if callback_func is not None:
            callback_func("Done")


class Banister:
    @info_time
    def __init__(
        self, pmc: PMC, metric, split_seasons=True, title=None, callback_func=None
    ):
        self.pmc = pmc
        self.metric = metric
        self.title = title
        if callback_func is not None:
            callback_func(f"Aggregating {metric.name}")
        response = pmc.athlete.aggregate_metric(
            metric, "max", callback_func=callback_func
        )
        self.activity.records_df = pmc.df
        first_date = response.index[0]
        index = pd.date_range(first_date, datetime.date.today())
        response = response.reindex(index, fill_value=np.nan)
        self.activity.records_df["response"] = response.mask(response == 0)

        def predict(row):
            predict = a + b * row["sts"] + c * row["lts"]
            future_predict = a + b * row["predicted_sts"] + c * row["predicted_lts"]
            return predict, future_predict

        res = []
        if split_seasons:
            for season in pmc.athlete.seasons:
                if callback_func is not None:
                    callback_func(f"Analysing season {season[0]} - {season[1]}")
                a, b, c = self.find_params(season)
                df = self.activity.records_df
                df = df.loc[season[0] <= df.index]
                if season != pmc.athlete.seasons[-1]:
                    df = df.loc[df.index <= season[1]]

                df["predict"], df["future_predict"] = zip(*df.apply(predict, axis=1))
                res.append(df)
            self.activity.records_df["predict"] = pd.concat(res)["predict"]
            self.activity.records_df["future_predict"] = pd.concat(res)[
                "future_predict"
            ]
        else:
            a, b, c = self.find_params()
            self.activity.records_df["predict"] = self.activity.records_df.apply(
                predict, axis=1
            )[0]
        if callback_func is not None:
            callback_func(f"Done")

    @debug_time
    def find_params(self, season=None):
        def func(idx, a, b, c):
            return (
                a
                + b * self.activity.records_df.loc[idx, "sts"]
                + c * self.activity.records_df.loc[idx, "lts"]
            )

        short_response = self.activity.records_df["response"].dropna()
        if season is not None:
            short_response = short_response.loc[season[0] <= short_response.index]
            short_response = short_response.loc[short_response.index <= season[1]]
        return scipy.optimize.curve_fit(func, short_response.index, short_response)[0]
