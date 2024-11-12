import pathlib
import typing
from sports_planner_lib.db.schemas import Record
import pandas as pd
import logging

from sqlalchemy.exc import IntegrityError

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
            f"Not importing these columns: {set(records_df) - set(needed_cols)}"
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

    def import_activity(
        self, athlete: "Athlete", metadata: dict, activity_file: pathlib.Path
    ) -> None:
        raise NotImplementedError


class LoginException(Exception):
    """Not logged in."""
