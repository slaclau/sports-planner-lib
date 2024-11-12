import logging
import pathlib

import yaml
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pandas as pd
from sports_planner_lib.db.schemas import Activity, Base, Record
from sports_planner_lib.importer.garmin import GarminImporter

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

    def get_activities(self):
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
                    logger.debug(f"Downloading {activity} from {importer}")
                    activity_file = importer_obj.download_activity(
                        activity["activity_id"],
                        self.dir / "downloaded_activities",
                        force=force,
                    )

                    importer_obj.import_activity(
                        self, activity, activity_file, force=force
                    )


if __name__ == "__main__":
    pd.options.mode.chained_assignment = None
    base_logger = logging.getLogger("")
    base_logger.setLevel(logging.DEBUG)
    athlete = Athlete("seb.laclau@gmail.com")
    # athlete.update_db(force=False)

    a = athlete.get_activities()

    with athlete.Session() as session:
        act = session.get(Activity, a[1].activity_id)
        print(act.records_df.index)