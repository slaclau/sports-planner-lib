import logging
import pathlib

import yaml
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pandas as pd
from sports_planner_lib.db.schemas import Activity, Base, Record
from sports_planner_lib.importer.garmin import GarminImporter
from sports_planner_lib.metrics.calculate import get_all_metrics, MetricsCalculator

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
            activities = session.query(Activity).all()
            return activities

    def update_db(self, force=False):
        importers = {"garmin": GarminImporter}
        for importer in self.config["importers"]:
            if "activities" in self.config["importers"][importer]["roles"]:
                logger.debug(f"Getting activities from {importer}")
                importer_obj = importers[importer](self.config["importers"][importer])
                activities = importer_obj.list_activities()
                for activity in activities:
                    with self.Session() as session:
                        if session.get(Activity, activity["activity_id"]) and not force:
                            continue
                    logger.debug(f"Downloading {activity} from {importer}")
                    activity_file = importer_obj.download_activity(
                        activity["activity_id"],
                        self.dir / "downloaded_activities",
                        force=force,
                    )

                    importer_obj.import_activity(
                        self, activity, activity_file, force=force
                    )
        self.update_metrics(recompute=force)

    def update_metrics(self, recompute=False):
        metrics = MetricsCalculator.order_deps(list(get_all_metrics()))
        for activity in self.activities:
            for metric in metrics:
                metric_instance = metric(activity)
                if metric_instance.is_applicable():
                    print(f"{metric.__name__}: {metric_instance.compute()}")


if __name__ == "__main__":
    pd.options.mode.chained_assignment = None
    base_logger = logging.getLogger("")
    base_logger.setLevel(logging.DEBUG)
    athlete = Athlete("seb.laclau@gmail.com")
    athlete.update_db(force=False)

    a = athlete.activities

    with athlete.Session() as session:
        act = session.get(Activity, a[0].activity_id)
        print(act.sessions)
