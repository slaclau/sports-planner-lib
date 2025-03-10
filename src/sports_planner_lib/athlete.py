import logging
import pathlib
import time

import pandas as pd
import sweat
import yaml
from matplotlib.style.core import available
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import selectinload, sessionmaker

from sports_planner_lib.db.schemas import Activity, Base, MeanMax, Metric, Record
from sports_planner_lib.importer.garmin import GarminImporter
from sports_planner_lib.metrics.calculate import MetricsCalculator, get_all_metrics
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

    def get_metric(self, activity, metric):
        with self.Session() as session:
            activity = session.get(Activity, activity.activity_id)
            return activity.get_metric(metric)

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
                        if (
                            session.get(Activity, activity["activity_id"])
                            and not redownload
                        ):
                            continue
                    logger.info(
                        f"Downloading {activity} from {importer}",
                        extra=dict(action="download", activity=activity, i=i, n=n),
                    )
                    activity_file = importer_obj.download_activity(
                        activity["activity_id"],
                        self.dir / "downloaded_activities",
                        force=redownload,
                    )

                    importer_obj.import_activity(
                        self, activity, activity_file, force=reimport
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
        for activity in self.activities:
            i += 1
            with self.Session() as session:
                activity = session.get(Activity, activity.activity_id)
                if len(activity.meanmaxes) > 0 and not recompute:
                    continue
                logger.info(
                    f"Getting mean max values for {activity.activity_id}",
                    extra=dict(action="get_meanmaxes", activity=activity, i=i, n=n),
                )
                time_now = time.time()
                records_df = activity.records_df
                available_cols = [
                    k
                    for k, v in records_df[source_cols].isnull().all().items()
                    if not v
                ]
                unused_cols = [
                    k for k, v in records_df[source_cols].isnull().all().items() if v
                ]
                logger.debug(f"Using {available_cols} of {source_cols}")
                df = records_df.sweat.mean_max(available_cols)
                df[["mean_max_" + col for col in unused_cols]] = None
                df["duration"] = df.index.total_seconds()
                rows = df.to_dict(orient="records")
                for row in rows:
                    if recompute:
                        session.merge(MeanMax(activity_id=activity.activity_id, **row))
                    else:
                        session.add(MeanMax(activity_id=activity.activity_id, **row))
                try:
                    session.commit()
                except IntegrityError:
                    pass
                logger.debug(f"Took {time.time() - time_now:0.2f} s")

    def update_metrics(self, recompute=False):
        metrics = get_all_metrics()
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
        for activity in self.activities:
            i += 1
            with self.Session() as session:
                activity = session.get(
                    Activity,
                    activity.activity_id,
                    options=[joinedload(Activity.records)],
                )
                logger.info(
                    f"computing metrics for {activity.activity_id}",
                    extra=dict(action="compute_metrics", activity=activity, i=i, n=n),
                )
                existing_metrics = [metric.name for metric in activity.metrics]
                for metric in metrics:
                    if metric.__name__ in existing_metrics and not recompute:
                        continue
                    metric_instance = metric(activity)
                    try:
                        if metric_instance.applicable():
                            value = metric_instance.compute()
                            if value is None:
                                continue
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
                    except IntegrityError as e:
                        session.rollback()


if __name__ == "__main__":
    pd.options.mode.chained_assignment = None
    logging.basicConfig(level=logging.DEBUG)

    athlete = Athlete("seb.laclau@gmail.com")
    athlete.import_activities(redownload=False, reimport=False)
    athlete.update_db(recompute=True)

    a = athlete.activities

    with athlete.Session() as session:
        act = session.get(Activity, a[0].activity_id)
        print(act.metrics)
