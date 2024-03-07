from typing import Any, List, Tuple

from pydantic import BaseModel, Field


class ScanConfig(BaseModel):
    scan_paths: List[str]
    database_filepath: str
    scan_period_wait_time_hours: int
    csv_dump_path: str
    log_file: str
    reports_path: str


class TableDescription(BaseModel):
    table_name: str
    file_path: str
    lock: Any  # threading.Lock
    columns: List[Tuple[str, str]]
    csv_dump_file: str
    primary_key: List[str]

    @property
    def columns_string(self) -> str:
        return ",".join(f"{v[0]} {v[1]}" for v in self.columns)

    @property
    def columns_placeholder_string(self) -> str:
        return ",".join(["?" for _ in range(len(self.columns))])

    @property
    def primary_key_string(self) -> str:
        if len(self.primary_key):
            return f", PRIMARY KEY ({','.join(self.primary_key)})"
        else:
            return ""


class FileStat(BaseModel):
    date__inode: str
    date_scanned: str
    date_modified: str
    date_created: str
    inode: int
    filename: str
    filepath: str
    filetype: str
    lines: int
    error_occured: bool
    error_traceback: str

    def to_tuple(self) -> Tuple[Any, ...]:
        return (
            self.date__inode,
            self.date_scanned,
            self.inode,
            self.filename,
            self.filepath,
            self.filetype,
            self.lines,
            self.date_created,
            self.date_modified,
            self.error_occured,
            self.error_traceback,
        )


class ScanStat(BaseModel):
    date_scanned: str
    scan_time: float
    files_scanned: int
    files_skipped: int

    def to_tuple(self) -> Tuple[Any, ...]:
        return (
            self.date_scanned,
            self.scan_time,
            self.files_scanned,
            self.files_skipped,
        )


class LineCountStat(BaseModel):
    count: int = Field(default=0)
    error_occured: bool = Field(default=False)
    error_traceback: str = Field(default="")
