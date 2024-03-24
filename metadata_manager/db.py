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

        # Create database schema if it doesn't exist.
        Base.metadata.create_all(engine)
        return Session()
    except Exception as exc:
        return exc
