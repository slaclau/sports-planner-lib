import logging
import pathlib
import typing

import pandas as pd
from matplotlib.style.core import available
from sqlalchemy.exc import IntegrityError

from sports_planner_lib.db.schemas import Lap, Record, Session
from sports_planner_lib.utils.logging import debug_time

if typing.TYPE_CHECKING:
    from sports_planner_lib.athlete import Athlete

logger = logging.getLogger(__name__)


class ActivityImporter:
    file_readers = {}

    def __init__(self, params: dict):
        raise NotImplementedError

    def list_activities(self) -> list[dict]:
        raise NotImplementedError

    def download_activity(
        self, activity_id: int, target_dir: pathlib.Path, force=False
    ) -> pathlib.Path:
        raise NotImplementedError

    @debug_time
    def _import_records_df(
        self,
        athlete: "Athlete",
        activity_id: int,
        records_df: pd.DataFrame,
        force=False,
    ):
        with athlete.Session() as session:
            if session.query(Record).filter(Record.activity_id == activity_id).count():
                if force:
                    session.query(Record).filter(
                        Record.activity_id == activity_id
                    ).delete()
                else:
                    return
        records_df["activity_id"] = activity_id
        needed_cols = Record.__table__.columns.keys()
        logger.debug(
            f"Not importing these columns from records df: {set(records_df) - set(needed_cols)}"
        )
        needed_cols.pop(needed_cols.index("timestamp"))

        cols = set(needed_cols).intersection(records_df.columns)
        cols = list(cols)

        df = records_df.loc[:, cols]
        df.loc[:, "timestamp"] = records_df.index
        df = df.reindex(columns=[*needed_cols, "timestamp"])
        rows = df.to_sql(
            name="records",
            con=athlete.engine,
            if_exists="append",
            index=False,
        )
        logger.debug(f"added {rows} rows to records table")

    @classmethod
    def _read_file(cls, activity_file: pathlib.Path) -> dict:
        try:
            return cls.file_readers[activity_file.suffix](activity_file)
        except KeyError:
            logger.warning(
                f"not reading unknown file with extension {activity_file.suffix}"
            )
            return {}

    def _import_laps_df(
        self,
        athlete: "Athlete",
        activity_id: int,
        laps_df: pd.DataFrame,
        force=False,
    ):
        with athlete.Session() as session:
            if session.query(Lap).filter(Lap.activity_id == activity_id).count():
                if force:
                    session.query(Lap).filter(Lap.activity_id == activity_id).delete()
                else:
                    return
        laps_df["activity_id"] = activity_id
        needed_cols = Lap.__table__.columns.keys()
        logger.debug(
            f"Not importing these columns from laps df: {set(laps_df) - set(needed_cols)}"
        )

        df = laps_df.reindex(columns=needed_cols)
        rows = df.to_sql(
            name="laps",
            con=athlete.engine,
            if_exists="append",
            index=False,
        )
        logger.debug(f"added {rows} rows to laps table")

    def _import_sessions_df(
        self,
        athlete: "Athlete",
        activity_id: int,
        sessions_df: pd.DataFrame,
        force=False,
    ):
        with athlete.Session() as session:
            if (
                session.query(Session)
                .filter(Session.activity_id == activity_id)
                .count()
            ):
                if force:
                    session.query(Session).filter(
                        Session.activity_id == activity_id
                    ).delete()
                else:
                    return
        sessions_df["activity_id"] = activity_id
        needed_cols = Session.__table__.columns.keys()
        logger.debug(
            f"Not importing these columns from sessions df: {set(sessions_df) - set(needed_cols)}"
        )

        df = sessions_df.reindex(columns=needed_cols)
        rows = df.to_sql(
            name="sessions",
            con=athlete.engine,
            if_exists="append",
            index=False,
        )
        logger.debug(f"added {rows} rows to sessions table")

    def import_activity(
        self, athlete: "Athlete", metadata: dict, activity_file: pathlib.Path
    ) -> None:
        raise NotImplementedError


class LoginException(Exception):
    """Not logged in."""
