from pathlib import Path

from sqlalchemy.orm import declarative_base, Mapped
from sqlalchemy import Column, String, UniqueConstraint

from models.types import PathType
from models.mixins import ORMReprMixin

Base = declarative_base()


class RepositoriesORM(Base, ORMReprMixin):
    __tablename__ = "repositories"

    id: Mapped[str] = Column(String, primary_key=True)
    path: Mapped[str | Path] = Column(PathType, nullable=False)

    __table_args__ = (UniqueConstraint("path", name="unique_repository"),)
