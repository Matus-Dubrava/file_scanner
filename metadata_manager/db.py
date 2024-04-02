from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from md_models import Base


def create_or_get_session(db_path: Path) -> Session | Exception:
    """
    Creates a new sqlite database if it doesn't exist and returns a
    session to the database.
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
