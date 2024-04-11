from datetime import datetime
from pathlib import Path

from sqlalchemy.orm import declarative_base, Mapped
from sqlalchemy import Column, String, DateTime

from models.types import PathType

Base = declarative_base()


class RespoitoriesORM(Base):
    __tablename__ = "repositories"

    id: Mapped[str] = Column(String, primary_key=True)
    path: Mapped[str | Path] = Column(PathType, nullable=False)
    date_created: Mapped[datetime] = Column(
        DateTime, nullable=False, default=datetime.now()
    )
