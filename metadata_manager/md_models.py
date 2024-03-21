from pydantic import BaseModel
import enum
from datetime import datetime

from sqlalchemy import Column, Integer, String, Enum, ForeignKey, DateTime
from sqlalchemy.orm import relationship, declarative_base
from sqlalchemy.sql.functions import current_timestamp

Base = declarative_base()


@enum.unique
class FileStatus(enum.Enum):
    ACTIVE = "ACTIVE"
    REMOVED = "REMOVED"
    UNTRACKED = "UNTRACKED"


class FileORM(Base):  # type: ignore
    __tablename__ = "file"

    filepath__git_branch = Column(String, unique=True, nullable=False, primary_key=True)
    filepath = Column(String, nullable=False)
    git_branch = Column(String, nullable=False, default="")
    timestamp_added = Column(DateTime, nullable=False, default=current_timestamp())
    fs_timestamp_created = Column(DateTime, nullable=False, default=current_timestamp())
    filename = Column(String, nullable=False)
    status = Column(Enum(FileStatus), name="status_enum")  # type: ignore

    history = relationship("History", back_populates="file")


class HistoryORM(Base):  # type: ignore
    __tablename__ = "history"

    id = Column(Integer, primary_key=True, autoincrement=True)
    filepath__git_branch = Column(
        String,
        ForeignKey("file.filepath__git_branch", ondelete="CASCADE"),
        nullable=False,
    )
    filepath = Column(String, nullable=False)
    git_branch = Column(String, nullable=False, default="")
    timestamp_record_added = Column(
        DateTime, nullable=False, default=current_timestamp()
    )
    fs_size = Column(Integer, nullable=False)
    fs_date_modified = Column(DateTime, nullable=False, default=current_timestamp())
    fs_inode = Column(Integer, nullable=False)
    n_total_lines = Column(Integer, nullable=False)
    n_changed_lines = Column(Integer, nullable=False)
    running_changed_lines = Column(Integer, nullable=False)
    file_hash = Column(String, nullable=False)

    file = relationship("File", back_populates="history")


class Config(BaseModel):
    md_dir_name: str
    md_db_name: str


class AppInfo(BaseModel):
    version: str
    commit: str
    build_type: str
    build_date: datetime
