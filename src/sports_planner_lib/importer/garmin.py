import datetime
import logging
import os.path
import pathlib
import typing
import zipfile

import garth
import sweat
import yaml
from dateutil import rrule
from garth.exc import GarthException
from matplotlib.style.core import available
from sqlalchemy import func
from sqlalchemy.exc import IntegrityError, StatementError

from sports_planner_lib.db.other import ConfiguredValue
from sports_planner_lib.db.schemas import (
    Activity,
    Record,
    UnknownMessage,
)
from sports_planner_lib.importer.base import ActivityImporter, LoginException
from sports_planner_lib.utils.serial import serialize_dict

if typing.TYPE_CHECKING:
    from sports_planner_lib.athlete import Athlete

logger = logging.getLogger(__name__)


class GarminImporter(ActivityImporter):
    records_column_name_map = {"unknown_90": "performance_condition"}
    message_type_map = {140: "firstbeat"}

    def __init__(self, params: dict):
        self.file_readers[".fit"] = self._read_fit_file

        email = params["email"]
        password = params["password"] if "password" in params else None
        try:
            garth.resume(f"~/.sports-planner/{email}")
        except FileNotFoundError:
            if password is not None:
                garth.login(email, password)
                garth.save(f"~/.sports-planner/{email}")
            else:
                raise LoginException
        try:
            garth.client.username
        except GarthException:
            raise LoginException

    def list_activities(self) -> list[dict]:
        """List activities from Garmin Connect."""

        url = "/activitylist-service/activities/search/activities"
        start = 0
        limit = 100

        rtn = []

        while True:
            activities = self._list_activities(start, limit)
            rtn = rtn + activities
            start = start + limit
            if len(activities) == 0:
                break

        return rtn

    def _list_activities(self, start, limit):
        url = "/activitylist-service/activities/search/activities"
        params = {"start": str(start), "limit": str(limit)}
        logger.debug(f"calling api with {params}")

        response = garth.connectapi(url, params=params)

        rtn = []

        for activity in response:
            try:
                rtn.append(
                    {
                        "activity_id": activity["activityId"],
                        "name": (
                            activity["activityName"]
                            if "activityName" in activity
                            else "Untitled"
                        ),
                        "orig": activity,
                    }
                )
            except KeyError:
                print(activity)
                raise ValueError

        return rtn

    def download_activity(
        self, activity_id: int, target_dir: pathlib.Path, force=False
    ) -> pathlib.Path:
        """Download the specified activity from Garmin Connect.

        Parameters
        ----------
        activity_id: int
            The activity id to download from Garmin Connect.
        target_dir: str
            The directory to download the file to.
        force: bool
            Whether to re-download and re-extract if the file already exists.
        """
        url = f"/download-service/files/activity/{activity_id}"

        zip_path = target_dir / f"{activity_id}.zip"
        if force or not os.path.isfile(zip_path) or not zipfile.is_zipfile(zip_path):
            with open(zip_path, "wb+") as f:
                file = garth.download(url)
                f.write(file)
            logger.info(f"Downloaded {activity_id}.zip")
        else:
            logger.info(f"{activity_id}.zip already exists")

        with zipfile.ZipFile(target_dir / f"{activity_id}.zip", "r") as zip_ref:
            activity_file = zip_ref.namelist()[0]
            if force or not os.path.isfile(target_dir / activity_file):
                zip_ref.extractall(target_dir)
                logger.debug(f"Extracted {activity_file}")
            else:
                logger.debug(f"{activity_file} already exists")
        return target_dir / activity_file

    def import_activity(
        self,
        athlete: "Athlete",
        metadata: dict,
        activity_file: pathlib.Path,
        force=False,
    ):
        activity = self._read_file(activity_file)
        if activity == {}:
            logger.warning(f"unable to import {activity_file}")
            return
        already_exists = False
        needed_cols = Record.__table__.columns.keys()
        needed_cols.pop(needed_cols.index("timestamp"))

        available_columns = list(
            set(needed_cols).intersection(activity["data"].columns)
        )

        with athlete.Session() as session:
            if force or not session.get(Activity, metadata["activity_id"]):
                logger.info(f"Importing {metadata["activity_id"]} from {activity_file}")
                if activity["activity"]["timestamp"] is None:
                    logger.error(
                        f"not importing activity {metadata["activity_id"]} with no timestamp"
                    )
                    return
                session.merge(
                    Activity(
                        activity_id=metadata["activity_id"],
                        total_timer_time=activity["activity"]["total_timer_time"],
                        timestamp=activity["activity"]["timestamp"],
                        name=metadata["name"],
                        source="garmin",
                        original_file=str(activity_file),
                        available_columns=available_columns,
                    )
                )
            else:
                already_exists = True
            session.commit()
        if force or not already_exists:
            self._import_records_df(
                athlete,
                metadata["activity_id"],
                activity["data"],
                force=force == True or (isinstance(force, list) and "records" in force),
            )
            self._import_laps_df(
                athlete,
                metadata["activity_id"],
                activity["laps"],
                force=force == True or (isinstance(force, list) and "laps" in force),
            )
            self._import_sessions_df(
                athlete,
                metadata["activity_id"],
                activity["sessions"],
                force=force == True
                or (isinstance(force, list) and "sessions" in force),
            )
            self._import_unknown_messages(
                athlete,
                metadata["activity_id"],
                activity["unknown_messages"],
                force=force == True
                or (isinstance(force, list) and "unknowns" in force),
            )

    def _import_unknown_messages(
        self,
        athlete: "Athlete",
        activity_id: int,
        unknown_messages: list[dict[str, str | dict[str, str | float | int]]],
        force=False,
    ):
        logger.debug(f"importing unknown messages from {activity_id}")
        with athlete.Session() as session:
            for unknown_message in unknown_messages:
                message_type = unknown_message["type"]
                if message_type not in [
                    "firstbeat",
                    "device_info",
                    "workout",
                    "workout_step",
                    "training_file",
                ]:
                    continue
                record = unknown_message["record"]
                if "timestamp" in record:
                    timestamp = record.pop("timestamp")
                else:
                    timestamp = None
                record = serialize_dict(record)
                if force:
                    session.merge(
                        UnknownMessage(
                            activity_id=activity_id,
                            timestamp=timestamp,
                            type=message_type,
                            record=record,
                        )
                    )
                else:
                    session.add(
                        UnknownMessage(
                            activity_id=activity_id,
                            timestamp=timestamp,
                            type=message_type,
                            record=record,
                        )
                    )
            try:
                session.commit()
            except IntegrityError:
                pass
            except StatementError:
                pass

    @staticmethod
    def _read_fit_file(activity_file: pathlib.Path) -> dict:
        if activity_file.suffix == ".fit":
            res = sweat.read_file(
                activity_file,
                summaries=True,
                resample=True,
                interpolate=True,
                hrv=True,
                unknown_messages=True,
            )
        else:
            raise RuntimeError(f"Invalid file type: {activity_file.suffix}")
        res["data"] = GarminImporter.standardize_df(
            res["data"], column_name_map=GarminImporter.records_column_name_map
        )
        for field in ["sessions", "laps"]:
            try:
                res[field] = GarminImporter.standardize_df(
                    res[field],
                    fractional=False,
                    column_name_map=GarminImporter.records_column_name_map,
                )
            except KeyError:
                pass
        try:
            unknowns = res["unknown_messages"]
            for unknown in unknowns:
                if unknown["type"] in GarminImporter.message_type_map:
                    unknown["type"] = GarminImporter.message_type_map[unknown["type"]]

            res["unknown_messages"] = unknowns
        except KeyError:
            pass
        return res

    @staticmethod
    def standardize_df(df, enhanced=True, fractional=True, column_name_map=None):
        for column in df.columns:
            if "enhanced_" in column and enhanced:
                base = column.replace("enhanced_", "")
                if base in df.columns:
                    df.drop(columns=[base], inplace=True)
                df.rename(columns={column: base}, inplace=True)

            if "fractional_" in column and fractional:
                base = column.replace("fractional_", "")
                df[base] += df[column]
                df.drop(columns=[column], inplace=True)
            df.rename(columns=column_name_map, inplace=True)

        return df

    def _get_biometric_history(
        self, athlete: "Athlete", field: str, start=None, end=None
    ):
        fieldmap = {
            "ftp": "functionalThresholdPower",
            "ltp": "lactateThresholdSpeed",
            "lthr": "lactateThresholdHeartRate",
        }
        suffix = ""
        if field == "ltp":
            suffix = "&sport=RUNNING"
        if start is None:
            with athlete.Session() as session:
                start = session.query(func.min(Activity.timestamp)).first()[0].date()

        if end is None:
            end = datetime.date.today()

        rtn = []
        for dt in rrule.rrule(rrule.YEARLY, dtstart=start, until=end):
            dt_end = min(end, dt.date() + datetime.timedelta(days=365))
            rtn += garth.connectapi(
                f"/biometric-service/stats/{fieldmap[field]}/range/{dt.date()}/{dt_end}?aggregation=daily"
                + suffix
            )

        return rtn

    def sync_biometric_history(self, athlete: "Athlete", field: str):
        biometrics = self._get_biometric_history(
            athlete, field, None, datetime.date.today()
        )

        with athlete.Session() as session:
            for biometric in biometrics:
                dt = datetime.datetime.strptime(biometric["from"], "%Y-%m-%d")

                if not session.get(
                    ConfiguredValue,
                    (
                        field,
                        biometric["series"],
                        dt,
                    ),
                ):
                    session.add(
                        ConfiguredValue(
                            name=field,
                            sport=biometric["series"],
                            date=dt,
                            value=biometric["value"],
                        )
                    )
                    logger.debug(
                        f"added {biometric["value"]} for {biometric["series"]} from {biometric["from"]}"
                    )

            session.commit()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    id = "seb.laclau@gmail.com"
    dir = pathlib.Path.home() / "sports-planner"
    with open(dir / id / f"config.yaml") as f:
        config = yaml.safe_load(f)

    garmin_importer = GarminImporter(config["importers"]["garmin"])
    # activities = garmin_importer.list_activities()
    from sports_planner_lib.athlete import Athlete

    athlete = Athlete(id)
    garmin_importer.sync_biometric_history(athlete, "ltp")
    garmin_importer.sync_biometric_history(athlete, "ftp")
    garmin_importer.sync_biometric_history(athlete, "lthr")
