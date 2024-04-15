from pathlib import Path
from datetime import datetime
from typing import Optional

from sqlalchemy.orm import declarative_base, Mapped, relationship, Session
from sqlalchemy import (
    Column,
    String,
    UniqueConstraint,
    Integer,
    DateTime,
    ForeignKey,
    Float,
)

from models.types import PathType
from models.mixins import ORMReprMixin


Base = declarative_base()


class RepositoriesORM(Base, ORMReprMixin):
    __tablename__ = "repositories"

    id: Mapped[str] = Column(String, primary_key=True)
    path: Mapped[str | Path] = Column(PathType, nullable=False)

    __table_args__ = (UniqueConstraint("path", name="unique__repository"),)


class RefreshLogORM(Base, ORMReprMixin):
    __tablename__ = "refresh_log"

    id: Mapped[str] = Column(String, primary_key=True)
    taken_at: Mapped[datetime] = Column(DateTime, nullable=False)
    duration = Column(
        Float, nullable=False
    )  # for some reason, "duration: Mapped[float]" doesn't work with mypy
    total_repositories: Mapped[int] = Column(Integer, nullable=False)
    repositories_refreshed: Mapped[int] = Column(Integer, nullable=False)
    repositories_refreshed_with_errors: Mapped[int] = Column(Integer, nullable=False)
    repositories_failed: Mapped[int] = Column(Integer, nullable=False)
    error_occured: Mapped[int] = Column(Integer, nullable=False, default=0)

    repositories: Mapped["RefreshRepositoryORM"] = relationship(
        "RefreshRepositoryORM", back_populates="log"
    )

    @staticmethod
    def get_latest(session: Session) -> Optional["RefreshLogORM"]:
        return (
            session.query(RefreshLogORM).order_by(RefreshLogORM.taken_at.desc()).first()
        )


class RefreshRepositoryORM(Base, ORMReprMixin):
    __tablename__ = "refresh_repository"

    id: Mapped[str] = Column(String, primary_key=True)
    refresh_id: Mapped[str] = Column(
        String, ForeignKey("refresh_log.id", ondelete="CASCADE"), nullable=False
    )
    repository_id: Mapped[str] = Column(String, nullable=False)
    path: Mapped[Path | str] = Column(PathType, nullable=False)
    total_files: Mapped[int | None] = Column(Integer, nullable=True, default=None)
    files_refreshed: Mapped[int | None] = Column(Integer, nullable=True, default=None)
    files_failed: Mapped[int | None] = Column(Integer, nullable=True, default=None)
    error_occured: Mapped[int] = Column(Integer, nullable=False, default=0)
    error: Mapped[str | None] = Column(String, nullable=True, default=None)
    error_tb: Mapped[str | None] = Column(String, nullable=True, default=None)

    log: Mapped[RefreshLogORM] = relationship(
        "RefreshLogORM", back_populates="repositories"
    )
    files: Mapped["RefreshFileORM"] = relationship(
        "RefreshFileORM", back_populates="repository"
    )


class RefreshFileORM(Base, ORMReprMixin):
    __tablename__ = "refresh_file"

    id: Mapped[str] = Column(String, primary_key=True)
    refresh_repository_id: Mapped[str] = Column(
        String, ForeignKey("refresh_repository.id", ondelete="CASCADE"), nullable=False
    )
    path: Mapped[str | Path] = Column(PathType, nullable=False)
    lines_added: Mapped[int | None] = Column(Integer, nullable=True, default=None)
    lines_removed: Mapped[int | None] = Column(Integer, nullable=True, default=None)
    running_lines_added: Mapped[int | None] = Column(
        Integer, nullable=True, default=None
    )
    running_lines_removed: Mapped[int | None] = Column(
        Integer, nullable=True, default=None
    )
    error_occured: Mapped[int] = Column(Integer, nullable=False, default=0)
    error: Mapped[str | None] = Column(String, nullable=True, default=None)
    error_tb: Mapped[str | None] = Column(String, nullable=True, default=None)

    repository: Mapped[RefreshRepositoryORM] = relationship(
        "RefreshRepositoryORM", back_populates="files"
    )

    __table_args__ = (
        UniqueConstraint(
            "refresh_repository_id", "path", name="unique__refresh_repository_id__path"
        ),
    )
