from pathlib import Path
import traceback
import sys
from typing import Any

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from models.local_models import Base as LocalBase
from models.global_models import Base as GlobalBase
import md_constants


def _create_or_get_session(db_path: Path, declarative_base: Any) -> Session | Exception:
    """
    Creates a new sqlite database if it doesn't exist and returns a
    session object.
    """
    try:
        db_url = f"sqlite:///{db_path}"
        engine = create_engine(str(db_url))
        Session = sessionmaker(bind=engine)

        declarative_base.metadata.create_all(engine)
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


def _get_session_or_exit(
    db_path: Path, declarative_base: Any, debug: bool = False
) -> Session:
    """
    Returns session object or exits if the connection cannot be established.
    """
    session_or_err = _create_or_get_session(
        db_path=db_path, declarative_base=declarative_base
    )

    if isinstance(session_or_err, Exception):
        if debug:
            print(f"{traceback.format_exception(session_or_err)}\n", file=sys.stderr)

        print(
            "fatal: failed to establish connection to internal database",
            file=sys.stderr,
        )
        sys.exit(md_constants.CANT_CREATE_SQLITE_SESSION)

    return session_or_err


def get_local_session_or_exit(db_path: Path, debug: bool = False) -> Session:
    """
    Retun session object connected to local repository database or exits if
    session can't be established.
    """
    return _get_session_or_exit(
        db_path=db_path, declarative_base=LocalBase, debug=debug
    )


def get_global_session_or_exit(db_path: Path, debug: bool = False) -> Session:
    """
    Retun session object connected to global repository database or exits if
    session can't be established.
    """
    return _get_session_or_exit(
        db_path=db_path, declarative_base=GlobalBase, debug=debug
    )


class LocalSession:
    def __init__(self, db_path: Path):
        self.db_path = db_path

    def __enter__(self) -> Session:
        session = _create_or_get_session(
            db_path=self.db_path, declarative_base=LocalBase
        )
        if isinstance(session, Exception):
            raise session

        self.session = session
        return self.session

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.session.commit()
        self.session.close()


class LocalSessionOrExit:
    def __init__(self, db_path: Path):
        self.db_path = db_path

    def __enter__(self) -> Session:
        self.session = get_local_session_or_exit(db_path=self.db_path)
        return self.session

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.session.commit()
        self.session.close()
