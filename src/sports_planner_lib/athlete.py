import datetime
import logging
import pathlib
import time

import numpy as np
import pandas as pd
import sweat
import yaml
from scipy.signal import argrelextrema
from sklearn.linear_model import LinearRegression
from sqlalchemy import and_, create_engine, func, text
from sqlalchemy.orm import selectinload, sessionmaker

from sports_planner_lib.db.schemas import Activity, Base, MeanMax, Metric, Record
from sports_planner_lib.importer.garmin import GarminImporter
from sports_planner_lib.metrics.calculate import (
    MetricsCalculator,
    get_all_metrics,
    parse_metric_string,
)
from sports_planner_lib.metrics.garmin import Firstbeat
from sports_planner_lib.metrics.pdm import Curve
from sports_planner_lib.metrics.pdm import MeanMax as MeanMaxMetric

logger = logging.getLogger(__name__)


class Athlete:
    """This class represents an athlete.

    It provides methods to access activities and workouts as well as for
    aggregating metrics.
    """

    def __init__(self, id):
        self.id = id
        self.dir = pathlib.Path.home() / "sports-planner" / id
        with open(self.dir / "config.yaml") as f:
            self.config = yaml.safe_load(f)
        self.engine = create_engine(f"sqlite:///{self.dir / "athlete.db"}")
        Base.metadata.create_all(self.engine)

        self.Session = sessionmaker(bind=self.engine)

    @property
    def activities(self):
        with self.Session() as session:
            activities = (
                session.query(Activity).options([selectinload(Activity.metrics)]).all()
            )
            return activities

    def get_metric(self, activity, metric, compute=True, query=True):
        with self.Session() as session:
            if isinstance(activity, Activity):
                activity = session.get(Activity, activity.activity_id)
            else:
                activity = session.get(Activity, activity)
            return activity.get_metric(
                metric, compute=compute, query=query, athlete=self
            )

    def get_metric_history(self, metric, sport=None, start_date=None, end_date=None):
        t1 = time.time()
        if start_date is None:
            start_date = min(
                [activity.timestamp.date() for activity in self.activities]
            )
        if end_date is None:
            end_date = datetime.date.today()
        with self.Session() as session:
            activities = (
                session.query(Activity.activity_id)
                .filter(
                    and_(
                        Activity.timestamp >= start_date,
                        Activity.timestamp < end_date + datetime.timedelta(days=1),
                    )
                )
                .all()
            )
            activities = [act for (act,) in activities]
        if sport is not None:
            activities = [
                activity
                for activity in activities
                if self.get_metric(activity, "Sport")["sport"] == sport
            ]
        metric_class, fields = parse_metric_string(metric)
        if not metric_class.cache:
            raise ValueError(f"{metric} is not cached")
        with self.Session() as session:
            metrics = (
                session.query(Metric)
                .options([selectinload(Metric.activity)])
                .filter(Metric.name == metric)
                .filter(Metric.activity_id.in_(activities))
                .all()
            )
        return metrics

    def get_mean_max_for_period(
        self,
        column,
        sport,
        start,
        end=datetime.date.today() + datetime.timedelta(days=1),
    ):
        with self.Session() as session:
            activities = session.query(Activity).where(
                and_(end > Activity.timestamp, Activity.timestamp >= start)
            )
            activities = [
                activity.activity_id
                for activity in activities
                if activity.get_metric("Sport")["sport"] == sport
            ]
            mean_max = (
                session.query(
                    MeanMax.duration,
                    MeanMax.activity_id,
                    func.max(text(f"meanmaxes.mean_max_{column}")),
                )
                .filter(MeanMax.activity_id.in_(activities))
                .group_by(MeanMax.duration)
            )

        return pd.DataFrame(
            mean_max, columns=["duration", "activity_id", f"mean_max_{column}"]
        )

    def get_bests_for_period(
        self,
        column,
        sport,
        start,
        end=datetime.date.today() + datetime.timedelta(days=1),
    ):
        full_df = self.get_mean_max_for_period(column, sport, start, end)
        df = full_df.copy()
        df["energy"] = df.duration * df[f"mean_max_{column}"]
        linear_df = df[120:1200]
        model = LinearRegression()
        model.fit(linear_df[["duration"]], linear_df["energy"])
        df["linear"] = model.predict(df[["duration"]])
        df["distance"] = df.energy - df.linear
        typical_CP = 261
        typical_WPrime = 15500
        typical_Pmax = 1100
        df["power_index"] = df[f"mean_max_{column}"] / (
            (
                typical_WPrime
                / (df.duration - (typical_WPrime / (typical_CP - typical_Pmax)))
            )
            + typical_CP
        )

        distance_idx = df.iloc[120:].groupby("activity_id")["distance"].idxmax()
        power_index_idx = df.iloc[1:120].groupby("activity_id")["power_index"].idxmax()
        idx = np.append([0], power_index_idx.values)
        idx = np.append(idx, distance_idx.values)
        return full_df.iloc[idx]

    def aggregate_metric(self, metric, sport=None, start_date=None, end_date=None):
        metric_class, fields = parse_metric_string(metric)
        metrics = self.get_metric_history(metric, sport, start_date, end_date)
        metrics = [
            (m.activity.activity_id, m.activity.timestamp, m.value) for m in metrics
        ]
        df = pd.DataFrame(metrics, columns=["activity_id", "timestamp", "value"])
        df.timestamp = pd.to_datetime(df.timestamp)
        df.timestamp = df.timestamp.dt.floor("d")
        return df.groupby("timestamp").apply(metric_class.aggregation_function)

    def get_activity_full(self, activity):
        with self.Session() as session:
            return session.get(
                Activity,
                activity.activity_id,
                options=[
                    selectinload(Activity.records),
                    selectinload(Activity.laps),
                    selectinload(Activity.sessions),
                    selectinload(Activity.metrics),
                    selectinload(Activity.unknown_messages),
                    selectinload(Activity.meanmaxes),
                ],
            )

    def import_activities(self, redownload=False, reimport=False):
        importers = {"garmin": GarminImporter}
        for importer in self.config["importers"]:
            if "activities" in self.config["importers"][importer]["roles"]:
                logger.info(f"Getting activities from {importer}")
                importer_obj = importers[importer](self.config["importers"][importer])
                activities = importer_obj.list_activities()
                i = 0
                n = len(activities)
                for activity in activities:
                    i += 1
                    with self.Session() as session:
                        imported_activity = session.get(
                            Activity, activity["activity_id"]
                        )
                    if not imported_activity:
                        logger.info(
                            f"Downloading {activity} from {importer}",
                            extra=dict(action="download", activity=activity, i=i, n=n),
                        )
                        activity_file = importer_obj.download_activity(
                            activity["activity_id"],
                            self.dir / "downloaded_activities",
                            force=redownload,
                        )
                    elif redownload:
                        logger.info(
                            f"Downloading {activity} from {importer} again",
                            extra=dict(action="download", activity=activity, i=i, n=n),
                        )
                        activity_file = importer_obj.download_activity(
                            activity["activity_id"],
                            self.dir / "downloaded_activities",
                            force=redownload,
                        )
                    if not imported_activity:
                        logger.info(
                            f"Importing {activity} from {importer}",
                            extra=dict(action="import", activity=activity, i=i, n=n),
                        )
                        importer_obj.import_activity(
                            self, activity, activity_file, force=reimport
                        )
                    elif reimport:
                        logger.info(
                            f"Importing {activity} from {importer} again",
                            extra=dict(action="import", activity=activity, i=i, n=n),
                        )
                        importer_obj.import_activity(
                            self,
                            activity,
                            pathlib.Path(imported_activity.original_file),
                            force=reimport,
                        )

    def update_db(self, recompute=False):
        logger.info("Updating database")
        self.update_meanmaxes(recompute=recompute)
        self.update_metrics(recompute=recompute)

    def update_meanmaxes(self, recompute=False):
        cols = MeanMax.__table__.columns.keys()
        cols.pop(cols.index("activity_id"))
        cols.pop(cols.index("duration"))

        source_cols = [col.replace("mean_max_", "") for col in cols]
        i = 0
        n = len(self.activities)

        with self.Session() as session:
            for activity in reversed(self.activities):
                i += 1
                time_now = time.time()
                if (
                    session.query(MeanMax)
                    .filter(MeanMax.activity_id == activity.activity_id)
                    .count()
                    > 0
                    and not recompute
                ):
                    logger.debug(
                        f"{activity.activity_id} already has meanmaxes",
                        extra=dict(action="get_meanmaxes", activity=activity, i=i, n=n),
                    )
                    continue
                logger.info(
                    f"Getting mean max values for {activity.activity_id}",
                    extra=dict(action="get_meanmaxes", activity=activity, i=i, n=n),
                )
                if (
                    session.query(Record)
                    .filter(Record.activity_id == activity.activity_id)
                    .count()
                ) == 0:
                    logger.error(f"{activity.activity_id} has no records")
                    continue
                activity = session.get(Activity, activity.activity_id)
                records_df = activity.records_df

                logger.debug(f"Took {time.time() - time_now:0.2f} s to get records df")
                available_cols = set(records_df.columns).intersection(set(source_cols))
                unused_cols = list(set(source_cols) - available_cols)
                logger.debug(f"Using {available_cols} of {source_cols}")
                df = records_df.sweat.mean_max(available_cols)
                df[["mean_max_" + col for col in unused_cols]] = None
                df["duration"] = df.index.total_seconds()
                logger.debug(
                    f"Took {time.time() - time_now:0.2f} s to create mean max df"
                )
                df["activity_id"] = activity.activity_id

                rows = df.to_sql(
                    name="meanmaxes",
                    con=self.engine,
                    if_exists="append",
                    index=False,
                )
                logger.debug(f"added {rows} rows to mean max table")
                logger.debug(f"Took {time.time() - time_now:0.2f} s to add rows")

    def update_metrics(self, recompute=False):
        metrics = get_all_metrics().copy()
        metrics.remove(Curve)
        metrics.remove(MeanMaxMetric)
        metrics.remove(Firstbeat)

        cols = MeanMax.__table__.columns.keys()
        cols.pop(cols.index("activity_id"))
        cols.pop(cols.index("duration"))

        source_cols = [col.replace("mean_max_", "") for col in cols]
        for col in source_cols:
            metrics.add(Curve[col])

        metrics = MetricsCalculator.order_deps(list(metrics))
        logger.debug(f"metrics: {[metric.__name__ for metric in metrics]}")

        i = 0
        n = len(self.activities)
        with self.Session() as session:
            for activity in reversed(self.activities):
                i += 1
                activity = session.get(
                    Activity,
                    activity.activity_id,
                )
                self._update_metrics_for_activity(
                    activity, metrics, i, n, recompute, session
                )

    def _update_metrics_for_activity(self, activity, metrics, i, n, recompute, session):
        logger.info(
            f"computing metrics for {activity.name} ({activity.activity_id})",
            extra=dict(action="compute_metrics", activity=activity, i=i, n=n),
        )
        existing_metrics = [metric.name for metric in activity.metrics]
        new_metrics = []

        for metric in metrics:
            if not metric.cache:
                logger.debug(f"skipping {metric.__name__} as can not be cached")
                continue
            if metric.__name__ in existing_metrics and not recompute:
                logger.debug(f"skipping {metric.__name__} as already computed")
                continue
            should_continue = False
            for dep in metric.deps:
                if (
                    dep.__name__ not in existing_metrics
                    and dep.__name__ not in new_metrics
                    and dep.cache
                ):
                    logger.debug(
                        f"skipping {metric.__name__} as dep {dep.__name__} is missing"
                    )
                    should_continue = True
                    break
            if should_continue:
                continue
            metric_instance = metric(activity, self)
            if metric_instance.applicable():
                logger.debug(f"computing {metric.__name__}")
                new_metrics.append(metric.__name__)
                try:
                    value = metric_instance.compute()
                except TypeError:
                    value = None
                except ValueError:
                    value = None
                except KeyError:
                    value = None
                if value is None:
                    session.merge(
                        Metric(
                            activity_id=activity.activity_id,
                            name=metric.__name__,
                            value=value,
                            json_value=None,
                        )
                    )
                try:
                    value = float(value)
                    session.merge(
                        Metric(
                            activity_id=activity.activity_id,
                            name=metric.__name__,
                            value=value,
                            json_value=None,
                        )
                    )
                except TypeError:
                    session.merge(
                        Metric(
                            activity_id=activity.activity_id,
                            name=metric.__name__,
                            value=None,
                            json_value=value,
                        )
                    )
                logger.debug(f"{metric}: {value}")
                session.commit()
            else:
                logger.debug(f"skipping {metric.__name__} as not applicable")
        if not new_metrics:
            logger.debug(f"{activity.activity_id} already has all metrics")
        else:
            logger.debug(f"added {new_metrics}")


if __name__ == "__main__":
    pd.options.mode.chained_assignment = None
    logging.basicConfig(level=logging.INFO)
    logger.setLevel(logging.DEBUG)
    # logging.getLogger("sports_planner_lib.importer.base").setLevel(logging.DEBUG)
    # logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)
    athlete = Athlete("seb.laclau@gmail.com")
    athlete.import_activities(redownload=False, reimport="unknowns")
    # athlete.update_db(recompute=False)
