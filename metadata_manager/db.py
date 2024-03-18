from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from md_models import Base


def create_db(dir: Path, db_name: str) -> Session:
    db_url = f"sqlite:///{dir / db_name}"
    engine = create_engine(str(db_url))
    Session = sessionmaker(bind=engine)
    Base.metadata.create_all(engine)
    return Session()
