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


class ORMReprMixin:
    def __repr__(self) -> str:
        class_ = self.__class__.__name__
        attrs = sorted((k, getattr(self, k)) for k in self.__mapper__.columns.keys())  # type: ignore
        str_attrs = ",\n".join(f"{key}={value!r}" for key, value in attrs)
        return f"{class_}({str_attrs})"


class FileORM(Base, ORMReprMixin):
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

    def clone(self) -> "FileORM":
        return FileORM(
            filepath=self.filepath,
            filename=self.filename,
            timestamp_added=self.timestamp_added,
            timestamp_deleted=self.timestamp_deleted,
            fs_timestamp_created=self.fs_timestamp_created,
            version_control_branch=self.version_control_branch,
            status=self.status,
        )

    def pretty_print(self, session: Session) -> None:
        latest_history_record = HistoryORM.get_latest(
            session=session, filepath=self.filepath
        )

        assert latest_history_record is not None, "Expected history record to exist."

        print(f"Path:\t\t\t{self.filepath}")
        print(f"Date Added:\t\t{self.timestamp_added}")
        print(f"Date Created:\t\t{self.fs_timestamp_created}")
        print(f"Date Modified:\t\t{latest_history_record.fs_date_modified}")
        print(f"Size:\t\t\t{latest_history_record.fs_size}")
        print(f"Line Count:\t\t{latest_history_record.count_total_lines}")
        print(f"Total Lines Added:\t{latest_history_record.running_added_lines}")
        print(f"Total Lines Removed:\t{latest_history_record.count_removed_lines}")


class HistoryORM(Base, ORMReprMixin):
    __tablename__ = "history"

    id: Mapped[str] = Column(String, primary_key=True)
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
    def get_latest(
        cls, session: Session, filepath: Path | str
    ) -> Optional["HistoryORM"]:
        return (
            session.query(cls)
            .filter_by(filepath=filepath)
            .order_by(HistoryORM.timestamp_record_added.desc())
            .first()
        )

    def clone(self) -> "HistoryORM":
        return HistoryORM(
            id=self.id,
            filepath=self.filepath,
            version_control_branch=self.version_control_branch,
            timestamp_record_added=self.timestamp_record_added,
            fs_size=self.fs_size,
            fs_date_modified=self.fs_date_modified,
            fs_inode=self.fs_inode,
            count_total_lines=self.count_total_lines,
            count_added_lines=self.count_added_lines,
            count_removed_lines=self.count_removed_lines,
            running_added_lines=self.running_added_lines,
            running_removed_lines=self.running_removed_lines,
            file_hash=self.file_hash,
        )

    def pretty_print(self) -> None:
        print(f"Recorded:\t\t{self.timestamp_record_added}")
        print(f"Branch:\t\t\t{self.version_control_branch}")
        print(f"Size:\t\t\t{self.fs_size}")
        print(f"Count Lines:\t\t{self.count_total_lines}")
        print(f"Total Lines Added:\t{self.running_added_lines}")
        print(f"Total Lines Removed:\t{self.running_removed_lines}")
        print(f"Lines Added:\t\t{self.count_added_lines}")
        print(f"Lines Removed:\t\t{self.count_removed_lines}")


class VersionInfoORM(Base, ORMReprMixin):
    __tablename__ = "version_info"

    commit_id: Mapped[str] = Column(String, nullable=False, primary_key=True)
    version: Mapped[str] = Column(String, nullable=False)
    build_type: Mapped[BuildType] = Column(
        Enum(BuildType), nullable=False, name="build_type_enum"
    )
    build_date: Mapped[datetime] = Column(DateTime, nullable=False)


class RepositoryORM(Base, ORMReprMixin):
    __tablename__ = "repository"

    id: Mapped[str] = Column(String, primary_key=True)
    date_created: Mapped[datetime] = Column(
        DateTime, nullable=False, default=datetime.now()
    )
    repository_filepath: Mapped[Path | str] = Column(PathType, nullable=False)
    parent_repository_id: Mapped[str] = Column(String, nullable=True)
    parent_repository_filepath: Mapped[Path | str] = Column(PathType, nullable=True)

    def pretty_print(self) -> None:
        print(f"Repository ID:\t\t{self.id}")
        print(f"Date Created:\t\t{self.date_created}")
        print(f"Repository Path:\t{self.repository_filepath}")
        print(f"Parent Repository ID:\t{self.parent_repository_id}")
        print(f"Parent Repository Path:\t{self.parent_repository_filepath}")


class FileListing(BaseModel):
    """
    Used to dump listing of tracked files into json.
    """

    repository_id: str
    repository_path: Path | str
    applied_status_filters: List[FileStatus]
    filepaths: List[Path | str]


class RepositoryStats(BaseModel):
    active_files_count: int
    removed_files_count: int
    total_lines_count: int
    added_lines_count: int
    removed_lines_count: int

    def pretty_print(self) -> None:
        print(f"Active Files Count:\t{self.active_files_count}")
        print(f"Removed Files Count:\t{self.removed_files_count}")
        print(f"Total Lines Count:\t{self.total_lines_count}")
        print(f"Added Lines Count:\t{self.added_lines_count}")
        print(f"Removed Lines Count:\t{self.removed_lines_count}")


class Config(BaseModel):
    md_dir_name: str
    md_db_name: str

    @staticmethod
    def from_file(path: Path) -> Union["Config", Exception]:
        try:
            return Config.model_validate_json(path.read_text())
        except Exception as exc:
            return exc


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
