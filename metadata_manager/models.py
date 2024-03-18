from sqlalchemy import Column, Integer, String, Enum, ForeignKey, DateTime
from sqlalchemy.orm import relationship, declarative_base
from sqlalchemy.sql.functions import current_timestamp
import enum

Base = declarative_base()


@enum.unique
class FileStatus(enum.Enum):
    ACTIVE = "ACTIVE"
    REMOVED = "REMOVED"
    UNTRACKED = "UNTRACKED"


class File(Base):  # type: ignore
    __tablename__ = "file"

    filepath__branch = Column(String, unique=True, nullable=False, primary_key=True)
    filepath = Column(String, nullable=False)
    branch = Column(String, nullable=False, default="")
    timestamp_added = Column(DateTime, nullable=False, default=current_timestamp())
    fs_timestamp_created = Column(DateTime, nullable=False)
    filename = Column(String, nullable=False)
    status = Column(Enum(FileStatus), name="status_enum")  # type: ignore

    history = relationship("History", back_populates="file")


class History(Base):  # type: ignore
    __tablename__ = "history"

    id = Column(Integer, primary_key=True, autoincrement=True)
    filepath__branch = Column(
        String, ForeignKey("file.filepath__branch", ondelete="CASCADE"), nullable=False
    )
    filepath = Column(String, nullable=False)
    branch = Column(String, nullable=False, default="")
    timestamp_created = Column(DateTime, nullable=False, default=current_timestamp())
    fs_size = Column(Integer, nullable=False)
    fs_date_modified = Column(DateTime, nullable=False)
    fs_inode = Column(Integer, nullable=False)
    n_total_lines = Column(Integer, nullable=False)
    n_changed_lines = Column(Integer, nullable=False)
    running_changed_lines = Column(Integer, nullable=False)
    file_hash = Column(String, nullable=False)

    file = relationship("File", back_populates="history")
