from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from md_models import Base


def create_or_get_session(db_path: Path) -> Session:
    """
    Creates a new sqlite database if it doesn't exist and returns a
    session to the database.
    """
    db_url = f"sqlite:///{db_path}"
    engine = create_engine(str(db_url))
    Session = sessionmaker(bind=engine)

    # Create database schema if it doesn't exist.
    Base.metadata.create_all(engine)
    return Session()
