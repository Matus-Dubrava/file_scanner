from pydantic import BaseModel, ConfigDict, field_validator
from pathlib import Path
from datetime import datetime
from typing import Union, Optional, List, Any

from sqlalchemy import (
    Column,
    Integer,
    String,
    Enum,
    ForeignKey,
    DateTime,
    UniqueConstraint,
)
from sqlalchemy.orm import relationship, declarative_base, Mapped, Session

from models.types import PathType
from models.mixins import ORMReprMixin
from md_enums import FileStatus, BuildType
from md_constants import GREEN, RESET, RED, YELLOW

Base = declarative_base()


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
    file_metadata: Mapped["FileMetadataORM"] = relationship(
        "FileMetadataORM", back_populates="file"
    )

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


class FileMetadataORM(Base, ORMReprMixin):
    __tablename__ = "file_metadata"

    id: Mapped[int] = Column(Integer, primary_key=True, autoincrement=True)
    filepath: Mapped[str | Path] = Column(
        PathType, ForeignKey("file.filepath", ondelete="CASCADE")
    )
    key: str = Column(String, nullable=False)
    value: str = Column(String, nullable=False)

    file: Mapped[FileORM] = relationship("FileORM", back_populates="file_metadata")

    __table_args__ = (
        UniqueConstraint("filepath", "key", name="unique__filepath__key"),
    )


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


class RepositoryMetadataORM(Base, ORMReprMixin):
    __tablename__ = "repository_metadata"

    id: Mapped[int] = Column(Integer, primary_key=True, autoincrement=True)
    key: Mapped[str] = Column(String, unique=True, nullable=False)
    value: Mapped[str] = Column(String, nullable=True)


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


class GlobalPaths(BaseModel):
    path: str
    db_name: str
    log_dirname: str
    info_log_filename: str
    debug_log_filename: str


class Config(BaseModel):
    local_dir_name: str
    local_db_name: str
    global_paths: GlobalPaths

    @staticmethod
    def from_file(path: Path) -> Union["Config", Exception]:
        try:
            config = Config.model_validate_json(path.read_text())

            if config.global_paths.path.startswith("~"):
                dir_path = Path(config.global_paths.path).expanduser()
            else:
                dir_path = Path(config.global_paths.path)

            config.global_paths.path = str(dir_path)
            return config
        except Exception as exc:
            return exc

    def get_global_dir_path(self) -> Path:
        return Path(self.global_paths.path)

    def get_global_db_path(self) -> Path:
        return Path(self.global_paths.path).joinpath(self.global_paths.db_name)

    def get_global_log_path(self) -> Path:
        return Path(self.global_paths.path).joinpath(self.global_paths.log_dirname)

    def get_global_info_log_filepath(self) -> Path:
        return self.get_global_log_path().joinpath(self.global_paths.info_log_filename)

    def get_global_debug_log_filepath(self) -> Path:
        return self.get_global_log_path().joinpath(self.global_paths.debug_log_filename)


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
        version_info_path = Path(__file__).parent.parent.joinpath("version.json")
        try:
            with open(version_info_path, "r") as f:
                version_info = VersionInfo.model_validate_json(f.read())
                return version_info
        except Exception as err:
            return err


class PathWithError(BaseModel):
    path: Path
    errors: List[Any]  # List of Exceptions

    @field_validator("errors")
    def validate_errors(cls, v):
        for error in v:
            if not isinstance(error, Exception):
                raise ValueError(
                    "All items in errors must be instances for Exception class"
                )
        return v


class LocalRefreshOutcome(BaseModel):
    successful_paths: List[Path]
    failed_paths: List[PathWithError]
    error: Optional[Any]  # Optional Exception

    @field_validator("error")
    def validate_errors(cls, v):
        if v and not isinstance(v, Exception):
            raise ValueError("Error must be either None or instance of Exception class")
        return v

    @staticmethod
    def new() -> "LocalRefreshOutcome":
        return LocalRefreshOutcome(successful_paths=[], failed_paths=[], error=None)

    def add_failed_path(self, path: Path, errors: List[Exception]) -> None:
        self.failed_paths.append(PathWithError(path=path, errors=errors))

    def add_successful_path(self, path: Path) -> None:
        self.successful_paths.append(path)


class GlobalRefreshOutcome(BaseModel):
    total_repositories: int
    refreshed_repositories: int
    refreshed_repositories_with_errors: int
    failed_repositories: int
    total_files: int
    refreshed_files: int
    failed_files: int
    duration: float

    @staticmethod
    def new() -> "GlobalRefreshOutcome":
        return GlobalRefreshOutcome(
            total_repositories=0,
            refreshed_repositories=0,
            refreshed_repositories_with_errors=0,
            failed_repositories=0,
            total_files=0,
            refreshed_files=0,
            failed_files=0,
            duration=0.0,
        )

    def pretty_print(self):
        print(f"total repositories:\t\t{self.total_repositories}")
        print(f"  {GREEN}refreshed:\t\t\t{self.refreshed_repositories}{RESET}")
        print(
            f"  {YELLOW}refreshed with errors:\t{self.refreshed_repositories_with_errors}{RESET}"
        )
        print(f"  {RED}failed:\t\t\t{self.failed_repositories}{RESET}")
        print()
        print(f"total files:\t\t\t{self.total_files}")
        print(f"  {GREEN}refreshed:\t\t\t{self.refreshed_files}{RESET}")
        print(f"  {RED}failed:\t\t\t{self.failed_files}{RESET}")

    def get_log_str(self) -> str:
        return (
            f"total repositories: {self.total_repositories}, "
            f"refreshed repositories: {self.refreshed_repositories}, "
            f"refreshed repositories with error: {self.refreshed_repositories_with_errors}, "
            f"failed repositories: {self.failed_repositories}, "
            f"total files: {self.total_files}, "
            f"refreshed files: {self.refreshed_files}, "
            f"failed files: {self.failed_files}"
        )
