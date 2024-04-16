from pathlib import Path
from datetime import datetime
from typing import Optional, Any

from sqlalchemy.orm import declarative_base, Mapped, relationship, Session
from sqlalchemy import (
    Column,
    String,
    UniqueConstraint,
    Integer,
    DateTime,
    ForeignKey,
    Float,
    text,
    Result,
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

    @staticmethod
    def get_file_changes_between_datetimes(
        session: Session, start: datetime, end: datetime
    ) -> Result[Any]:
        # TODO: can't use the start time directly, need to ensure that there
        # are at least two refresh log records in the specified time frame.
        # * if there is no refresh log record within specified time frame, return empty list
        # * if there is just one refresh log record, expand the time frame into past to include
        #   the previous log record
        #   - if there is no previous record, meaning that the one that we have found is the first log record,
        #     don't subtract the running counts, just return the ones found in this one

        sql = """
            WITH files_with_first_last_taken_at
            AS (
                SELECT 
                    rf.id AS refresh_file_id,
                    rf.path AS file_path,
                    rr.path AS repository_path,
                    rf.running_lines_added AS running_lines_added,
                    rf.running_lines_removed AS running_lines_removed, 
                    rl.taken_at as taken_at,
                    MIN(rl.taken_at) OVER (PARTITION BY rf.path) AS first_taken_at,
                    MAX(rl.taken_at) OVER (PARTITION BY rf.path) AS last_taken_at
                FROM refresh_log AS rl
                JOIN refresh_repository AS rr
                    ON rl.id = rr.refresh_id
                JOIN refresh_file AS rf
                    ON rr.id = rf.refresh_repository_id
                WHERE rf.error_occured = 0
                    AND rl.taken_at >= :start_date
                    AND rl.taken_at <= :end_date
            ),
            first_running_lines 
            AS (
                SELECT
                    refresh_file_id,
                    file_path,
                    repository_path,
                    running_lines_added,
                    running_lines_removed,
                    taken_at
                FROM files_with_first_last_taken_at
                WHERE taken_at = first_taken_at
            ),
            last_running_lines 
            AS (
                SELECT
                    refresh_file_id,
                    file_path,
                    repository_path,
                    running_lines_added,
                    running_lines_removed,
                    taken_at
                FROM files_with_first_last_taken_at
                WHERE taken_at = last_taken_at
            )

            SELECT 
                f.file_path,
                f.repository_path,
                -- Note about these case statements. This is used to handle edge cases 
                -- when within the specified period, the file was recreated and the first 
                -- refresh record has the old counts while the last refresh record has the
                -- new counts. This is mostly a workaround just to avoid negative values but 
                -- in such case, counts will be incorrect for the specified period. 
                -- This is not a critical problem for now because of how this function is intended
                -- to be used but it is something to keep in mind.
                CASE 
                    WHEN l.running_lines_added - f.running_lines_added >= 0 
                    THEN l.running_lines_added - f.running_lines_added
                    ELSE 0
                END AS running_lines_added,
                CASE 
                    WHEN l.running_lines_removed - f.running_lines_removed >= 0 
                    THEN l.running_lines_removed - f.running_lines_removed
                    ELSE 0
                END AS running_lines_removed 
            FROM first_running_lines as f
            JOIN last_running_lines as l
                ON f.file_path = l.file_path
        """

        return session.execute(text(sql), {"start_date": start, "end_date": end})


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
