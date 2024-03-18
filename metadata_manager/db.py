from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from models import Base  # type: ignore


def create_db(dir: Path, db_name: str = "metadata.db") -> Session:
    db_url = f"sqlite:///{dir / db_name}"
    engine = create_engine(str(db_url))
    Session = sessionmaker(bind=engine)
    Base.metadata.create_all(engine)
    return Session()
