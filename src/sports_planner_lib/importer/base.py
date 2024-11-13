import logging
import pathlib
import typing

import pandas as pd
from sqlalchemy.exc import IntegrityError

from sports_planner_lib.db.schemas import Lap, Record, Session

if typing.TYPE_CHECKING:
    from sports_planner_lib.athlete import Athlete

logger = logging.getLogger(__name__)


class ActivityImporter:
    def __init__(self, params: dict):
        raise NotImplementedError

    def list_activities(self) -> list[dict]:
        raise NotImplementedError

    def download_activity(
        self, activity_id: int, target_dir: pathlib.Path, force=False
    ) -> pathlib.Path:
        raise NotImplementedError

    def _import_records_df(
        self,
        athlete: "Athlete",
        activity_id: int,
        records_df: pd.DataFrame,
        force=False,
    ):
        records_df["activity_id"] = activity_id
        needed_cols = Record.__table__.columns.keys()
        logger.debug(
            f"Not importing these columns from records df: {set(records_df) - set(needed_cols)}"
        )
        needed_cols.pop(needed_cols.index("timestamp"))
        df = records_df[needed_cols]
        df["timestamp"] = records_df.index
        rows = df.to_dict(orient="records")
        with athlete.Session() as session:
            for row in rows:
                if force:
                    session.merge(Record(**row))
                else:
                    session.add(Record(**row))
            try:
                session.commit()
            except IntegrityError:
                pass

    def _import_laps_df(
        self,
        athlete: "Athlete",
        activity_id: int,
        laps_df: pd.DataFrame,
        force=False,
    ):
        laps_df["activity_id"] = activity_id
        needed_cols = Lap.__table__.columns.keys()
        logger.debug(
            f"Not importing these columns from laps df: {set(laps_df) - set(needed_cols)}"
        )

        df = laps_df[needed_cols]
        rows = df.to_dict(orient="records")
        with athlete.Session() as session:
            for row in rows:
                if force:
                    session.merge(Lap(**row))
                else:
                    session.add(Lap(**row))
            try:
                session.commit()
            except IntegrityError:
                pass

    def _import_sessions_df(
        self,
        athlete: "Athlete",
        activity_id: int,
        sessions_df: pd.DataFrame,
        force=False,
    ):
        sessions_df["activity_id"] = activity_id
        needed_cols = Session.__table__.columns.keys()
        logger.debug(
            f"Not importing these columns from sessions df: {set(sessions_df) - set(needed_cols)}"
        )

        df = sessions_df[needed_cols]
        rows = df.to_dict(orient="records")
        with athlete.Session() as session:
            for row in rows:
                if force:
                    session.merge(Session(**row))
                else:
                    session.add(Session(**row))
            try:
                session.commit()
            except IntegrityError:
                pass

    def import_activity(
        self, athlete: "Athlete", metadata: dict, activity_file: pathlib.Path
    ) -> None:
        raise NotImplementedError


class LoginException(Exception):
    """Not logged in."""
