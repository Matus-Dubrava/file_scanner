from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from md_models import Base


def create_db(db_path) -> Session:
    db_url = f"sqlite:///{db_path}"
    engine = create_engine(str(db_url))
    Session = sessionmaker(bind=engine)
    Base.metadata.create_all(engine)
    return Session()


def get_session(db_path: Path) -> Session:
    """
    Establishes connection to sqlite database and returns session object.
    """
    db_url = f"sqlite:///{db_path}"
    engine = create_engine(db_url)
    return sessionmaker(bind=engine)()
