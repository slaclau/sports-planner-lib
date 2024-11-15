import logging
import os.path
import pathlib
import typing
import zipfile

import garth
import sweat
import yaml
from garth.exc import GarthException

from sports_planner_lib.db.schemas import Activity, UnknownMessage
from sports_planner_lib.importer.base import ActivityImporter, LoginException
from sports_planner_lib.utils.serial import serialize_dict

if typing.TYPE_CHECKING:
    from sports_planner_lib.athlete import Athlete

logger = logging.getLogger(__name__)


class GarminImporter(ActivityImporter):
    records_column_name_map = {"unknown_90": "performance_condition"}
    message_type_map = {140: "firstbeat"}

    def __init__(self, params: dict):
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
        limit = 5

        params = {"start": str(start), "limit": str(limit)}

        return [
            {
                "activity_id": activity["activityId"],
                "name": activity["activityName"],
                "orig": activity,
            }
            for activity in garth.connectapi(url, params=params)
        ]

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
        if force or not os.path.isfile(zip_path):
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
                logging.debug(f"Extracted {activity_file}")
            else:
                logging.debug(f"{activity_file} already exists")
        return target_dir / activity_file

    def import_activity(
        self,
        athlete: "Athlete",
        metadata: dict,
        activity_file: pathlib.Path,
        force=False,
    ):
        activity = self._read_fit_file(activity_file)
        already_exists = False
        with athlete.Session() as session:
            if not session.get(Activity, metadata["activity_id"]):
                logger.info(f"Importing {metadata["activity_id"]} from {activity_file}")
                session.add(
                    Activity(
                        activity_id=metadata["activity_id"],
                        total_timer_time=activity["activity"]["total_timer_time"],
                        timestamp=activity["activity"]["timestamp"],
                        name=metadata["name"],
                        source="garmin",
                        original_file=str(activity_file),
                    )
                )
            elif force:
                logger.warning(f"Potentially re-importing {metadata["activity_id"]}")
                session.merge(
                    Activity(
                        activity_id=metadata["activity_id"],
                        timestamp=activity["activity"]["timestamp"],
                        total_timer_time=activity["activity"]["total_timer_time"],
                        name=metadata["name"],
                        source="garmin",
                        original_file=str(activity_file),
                    )
                )
            else:
                already_exists = True
            session.commit()
        if force or not already_exists:
            self._import_records_df(
                athlete, metadata["activity_id"], activity["data"], force=force
            )
            self._import_laps_df(
                athlete, metadata["activity_id"], activity["laps"], force=force
            )
            self._import_sessions_df(
                athlete, metadata["activity_id"], activity["sessions"], force=force
            )
            self._import_unknown_messages(
                athlete,
                metadata["activity_id"],
                activity["unknown_messages"],
                force=force,
            )

    def _import_unknown_messages(
        self,
        athlete: "Athlete",
        activity_id: int,
        unknown_messages: list[dict[str, str | dict[str, str | float | int]]],
        force=False,
    ):
        with athlete.Session() as session:
            for unknown_message in unknown_messages:
                message_type = unknown_message["type"]
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
            raise RuntimeError("Invalid file type")
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


if __name__ == "__main__":
    id = "seb.laclau@gmail.com"
    dir = pathlib.Path.home() / "sports-planner"
    with open(dir / id / f"config.yaml") as f:
        config = yaml.safe_load(f)

    garmin_importer = GarminImporter(config["importers"]["garmin"])
    print(garmin_importer.list_activities())
