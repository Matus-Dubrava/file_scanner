from pydantic import BaseModel, ConfigDict
from pathlib import Path
from datetime import datetime
from typing import Any, Union, Optional, List

from sqlalchemy import Column, Dialect, Integer, String, Enum, ForeignKey, DateTime
from sqlalchemy.orm import relationship, declarative_base, Mapped, Session
from sqlalchemy.types import TypeDecorator

from md_enums import FileStatus, BuildType

Base = declarative_base()


class PathType(TypeDecorator):
    impl = String

    cache_ok = True

    def process_bind_param(self, value: Any | None, dialect: Dialect) -> Any:
        if isinstance(value, Path):
            return str(value)
        return value

    def process_result_value(self, value: Any | None, dialect: Dialect) -> Any | None:
        return Path(value) if value else None


class FileORM(Base):
    __tablename__ = "file"

    filepath: Mapped[Path | str] = Column(
        PathType, unique=True, nullable=False, primary_key=True
    )
    filename: Mapped[str] = Column(String, nullable=False)
    timestamp_added: Mapped[datetime] = Column(
        DateTime, nullable=False, default=datetime.now()
    )
    timestamp_deleted: Mapped[datetime] = Column(DateTime, nullable=True)
    fs_timestamp_created: Mapped[datetime] = Column(
        DateTime, nullable=False, default=datetime.now()
    )
    version_control_branch: Mapped[Optional[str]] = Column(String, nullable=True)
    status: Mapped[FileStatus] = Column(Enum(FileStatus), name="status_enum")

    history: Mapped["HistoryORM"] = relationship("HistoryORM", back_populates="file")


class HistoryORM(Base):
    __tablename__ = "history"

    id: Mapped[int] = Column(Integer, primary_key=True, autoincrement=True)
    filepath: Mapped[Path | str] = Column(
        PathType,
        ForeignKey("file.filepath", ondelete="CASCADE"),
        nullable=False,
    )
    version_control_branch: Mapped[Optional[str]] = Column(String, nullable=True)
    timestamp_record_added: Mapped[datetime] = Column(
        DateTime, nullable=False, default=datetime.now()
    )
    fs_size: Mapped[int] = Column(Integer, nullable=False)
    fs_date_modified: Mapped[datetime] = Column(
        DateTime, nullable=False, default=datetime.now()
    )
    fs_inode: Mapped[int] = Column(Integer, nullable=False)
    count_total_lines: Mapped[int] = Column(Integer, nullable=False)
    count_added_lines: Mapped[int] = Column(Integer, nullable=False)
    count_removed_lines: Mapped[int] = Column(Integer, nullable=False)
    running_added_lines: Mapped[int] = Column(Integer, nullable=False)
    running_removed_lines: Mapped[int] = Column(Integer, nullable=False)
    file_hash: Mapped[str] = Column(String, nullable=False)

    file: Mapped[FileORM] = relationship("FileORM", back_populates="history")

    @classmethod
    def get_latest(cls, session: Session) -> Optional["HistoryORM"]:
        return session.query(cls).order_by(HistoryORM.id.desc()).first()


class VersionInfoORM(Base):
    __tablename__ = "version_info"

    commit_id: Mapped[str] = Column(String, nullable=False, primary_key=True)
    version: Mapped[str] = Column(String, nullable=False)
    build_type: Mapped[BuildType] = Column(
        Enum(BuildType), nullable=False, name="build_type_enum"
    )
    build_date: Mapped[datetime] = Column(DateTime, nullable=False)


class Config(BaseModel):
    md_dir_name: str
    md_db_name: str


class FileStat(BaseModel):
    n_lines: int
    hashes: List[str]
    file_hash: str

    @staticmethod
    def new() -> "FileStat":
        return FileStat(n_lines=0, hashes=[], file_hash="")


class LineChanges(BaseModel):
    lines_added: int
    lines_removed: int

    @staticmethod
    def new() -> "LineChanges":
        return LineChanges(lines_added=0, lines_removed=0)


class VersionInfo(BaseModel):
    model_config = ConfigDict(use_enum_values=True)

    version: str
    commit_id: str
    build_type: BuildType
    build_date: datetime

    @staticmethod
    def get_info() -> Union["VersionInfo", Exception]:
        version_info_path = Path(__file__).parent / "version.json"
        try:
            with open(version_info_path, "r") as f:
                version_info = VersionInfo.model_validate_json(f.read())
                return version_info
        except Exception as err:
            return err
