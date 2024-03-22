from pydantic import BaseModel, ConfigDict
from pathlib import Path
from datetime import datetime
from typing import Union

from sqlalchemy import Column, Integer, String, Enum, ForeignKey, DateTime
from sqlalchemy.orm import relationship, declarative_base, Mapped
from sqlalchemy.sql.functions import current_timestamp

from md_enums import FileStatus, BuildType

Base = declarative_base()


class FileORM(Base):
    __tablename__ = "file"

    filepath: Mapped[str] = Column(
        String, unique=True, nullable=False, primary_key=True
    )
    version_control_branch = Column(String, nullable=True)
    timestamp_added = Column(DateTime, nullable=False, default=current_timestamp())
    fs_timestamp_created = Column(DateTime, nullable=False, default=current_timestamp())
    filename = Column(String, nullable=False)
    status = Column(Enum(FileStatus), name="status_enum")

    history: Mapped["HistoryORM"] = relationship("HistoryORM", back_populates="file")


class HistoryORM(Base):
    __tablename__ = "history"

    id = Column(Integer, primary_key=True, autoincrement=True)
    filepath = Column(
        String,
        ForeignKey("file.filepath", ondelete="CASCADE"),
        nullable=False,
    )
    version_control_branch = Column(String, nullable=False, default="")
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

    file: Mapped[FileORM] = relationship("FileORM", back_populates="history")


class Config(BaseModel):
    md_dir_name: str
    md_db_name: str


class VersionInfoORM(Base):
    __tablename__ = "version_info"

    commit_id = Column(String, nullable=False, primary_key=True)
    version = Column(String, nullable=False)
    build_type = Column(Enum(BuildType), nullable=False, name="build_type_enum")
    build_date = Column(DateTime, nullable=False)


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
