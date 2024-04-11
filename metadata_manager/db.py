from pathlib import Path
import traceback
import sys

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from models.local_models import Base
import md_constants


def create_or_get_session(db_path: Path) -> Session | Exception:
    """
    Creates a new sqlite database if it doesn't exist and returns a
    session object.
    """
    try:
        db_url = f"sqlite:///{db_path}"
        engine = create_engine(str(db_url))
        Session = sessionmaker(bind=engine)

        Base.metadata.create_all(engine)
        session = Session()

        # Need more testing around this, does it speed up the process
        # enough that it would be worth it?
        # NOTE: journal_mode is set to OFF during tests, no need to
        # keep track of journal there.
        # session.execute(text("PRAGMA journal_mode=MEMORY"))
        # session.execute(text("PRAGMA synchronous=OFF"))
        return session
    except Exception as exc:
        return exc


def get_session_or_exit(db_path: Path, debug: bool = False) -> Session:
    """
    Returns session object or exits if the connection cannot be established.
    """
    session_or_err = create_or_get_session(db_path=db_path)

    if isinstance(session_or_err, Exception):
        if debug:
            print(f"{traceback.format_exception(session_or_err)}\n", file=sys.stderr)

        print(
            "fatal: failed to establish connection to internal database",
            file=sys.stderr,
        )
        sys.exit(md_constants.CANT_CREATE_SQLITE_SESSION)

    return session_or_err
