import os
from typing import List, Any, Optional, Tuple
import traceback
from pydantic import BaseModel, Field
import apsw
from datetime import datetime, timedelta
import threading
import pandas as pd
import subprocess
import time
import logging
from logging.handlers import RotatingFileHandler
import shutil

CONFIG_PATH = "/home/matus/blob/config.json"
REPORTS_DIR = "/home/matus/blob/reports"
LOG_PATH = "/home/matus/blob/scan.log"

# ====================================== LOGGING ====================================================
logger = logging.getLogger("scan_logger")
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
console_handler.setFormatter(formatter)

file_handler = RotatingFileHandler(LOG_PATH, maxBytes=5 * 1024 * 1024, backupCount=2)
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)

logger.addHandler(console_handler)
logger.addHandler(file_handler)
# ===================================================================================================


class ScanConfig(BaseModel):
    scan_paths: List[str]
    database_filepath: str
    scan_period_wait_time_hours: int


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


TRACKING_TABLES = {
    "file": TableDescription(
        table_name="file",
        file_path="/home/matus/blob/file_records.dat",
        lock=threading.Lock(),
        columns=[
            ("date__inode", "TEXT"),  # PK shortcut - date + inode
            ("date_scanned", "DATETIME"),
            ("inode", "INTEGER"),
            ("filename", "TEXT"),
            ("filepath", "TEXT"),
            ("filetype", "TEXT"),
            ("lines", "INTEGER"),
            ("date_created", "DATETIME"),
            ("date_modified", "DATETIME"),
            ("error_occured", "INTEGER"),
            ("error_traceback", "TEXT"),
        ],
        primary_key=["date__inode"],
        csv_dump_file="/home/matus/blob/files.csv",
    ),
    "scan": TableDescription(
        table_name="scan",
        file_path="/home/matus/blob/file_records.dat",
        lock=threading.Lock(),
        columns=[
            ("date_scanned", "DATETIME"),
            ("scan_time", "REAL"),
            ("files_scanned", "INTEGER"),
            ("files_skipped", "INTEGER"),
        ],
        primary_key=[],
        csv_dump_file="/home/matus/blob/scans.csv",
    ),
}


def get_sqlite_conn(filepath: str) -> Tuple[apsw.Connection, Any]:
    conn = apsw.Connection(filepath)
    curs = conn.cursor()
    return conn, curs


class SqliteCursorWithLock:
    def __init__(self, filepath: str, lock: threading.Lock) -> None:
        self.filepath: str = filepath
        self.lock: threading.Lock = lock
        self.conn: Optional[apsw.Connection] = None
        self.curs: Optional[Any] = None

    def __enter__(self) -> Any:
        self.lock.acquire()
        self.conn, self.curs = get_sqlite_conn(filepath=self.filepath)
        return self.curs

    def __exit__(self, ext_type: Any, ext_val: Any, ext_tb: Any) -> None:
        self.lock.release()
        if self.curs is not None:
            self.curs.close()
            self.curs = None
        if self.conn is not None:
            self.conn.close()
            self.conn = None


def get_sqlite_datetime(datetime: datetime) -> str:
    return datetime.strftime("%Y-%m-%d %H:%M:%S")


def datetime_from_sqlite_datetime(sqlite_datetime: str) -> datetime:
    return datetime.strptime(sqlite_datetime, "%Y-%m-%d %H:%M:%S")


def dump_db_to_csv(scan_config: ScanConfig):
    for table in TRACKING_TABLES.values():
        conn, curs = get_sqlite_conn(filepath=scan_config.database_filepath)
        sql = f"""
            SELECT * FROM {table.table_name}
        """
        curs.execute(sql)
        try:
            df = pd.read_sql(sql, conn)
            df.to_csv(table.csv_dump_file, index=False, header=True)
        except apsw.ExecutionCompleteError:
            # Apparently, this exception will be thrown when pandas
            # is tring to get column description (curs.description) when
            # no recrods exist in the queried table. Therefore we are skipping
            # this error.
            logger.debug("Skipping write to csv. No records found.")

        curs.close()
        conn.close()


class LineCountStat(BaseModel):
    count: int = Field(default=0)
    error_occured: bool = Field(default=False)
    error_traceback: str = Field(default="")


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


def get_line_count(filename: str) -> LineCountStat:
    error_occured = False
    error_traceback = ""
    line_count = 0

    try:
        with open(filename) as f:
            for _ in f:
                line_count += 1
    except Exception:
        error_occured = True
        error_traceback = str(traceback.format_exc()).replace("\n", ",")

    return LineCountStat(
        count=line_count, error_occured=error_occured, error_traceback=error_traceback
    )


def is_tracked(filepath: str) -> bool:
    result = subprocess.run(
        ["getfattr", "--only-values", "-n", "user.tracked", filepath],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    return result.stdout.strip() == "true"


def create_tracking_tables(scan_config: ScanConfig) -> None:
    conn, curs = get_sqlite_conn(filepath=scan_config.database_filepath)

    for table in TRACKING_TABLES.values():
        sql = f"""
            CREATE TABLE IF NOT EXISTS {table.table_name} (
                {table.columns_string}
                {table.primary_key_string}
            )
        """
        curs.execute(sql)

    conn.close()
    curs.close()


def insert_data(table: TableDescription, records: List[Any]) -> None:
    sql = f"""
        INSERT INTO {table.table_name} VALUES ({table.columns_placeholder_string})
    """

    with SqliteCursorWithLock(filepath=table.file_path, lock=table.lock) as curs:
        curs.execute("begin")
        curs.executemany(sql, [record.to_tuple() for record in records])
        curs.execute("commit")


def create_daily_file_report():
    daily_report_total = os.path.join(REPORTS_DIR, "daily_total.csv")
    daily_report_by_type = os.path.join(REPORTS_DIR, "daily_by_type.csv")

    if os.path.exists(REPORTS_DIR):
        shutil.rmtree(REPORTS_DIR)

    os.makedirs(REPORTS_DIR)

    conn, curs = get_sqlite_conn(TRACKING_TABLES["file"].file_path)

    sql = f"""
        SELECT
            DATE(date_modified) as date,
            filetype,
            SUM(lines) as lines,
            COUNT(*) as count_files
        FROM {TRACKING_TABLES["file"].table_name}
        GROUP BY 
            DATE(date_modified),
            filetype
        ORDER BY 
            DATE(date_modified)
    """

    df = pd.read_sql(sql, conn)
    df.to_csv(daily_report_by_type, index=False, header=True)

    sql = f"""
        SELECT
            DATE(date_modified) as date,
            SUM(lines) as lines,
            COUNT(*) as count_files
        FROM {TRACKING_TABLES["file"].table_name}
        GROUP BY
            DATE(date_modified)
    """

    df = pd.read_sql(sql, conn)
    df.to_csv(daily_report_total, index=False, header=True)

    curs.close()
    conn.close()


def collect_file_stats(filepath: str, scan_start_time: datetime) -> FileStat:
    error_occured = False
    error_tracebacks = []
    filename = os.path.basename(filepath)
    inode = 0
    date_created = 0.0
    date_modified = 0.0
    line_count_stat = LineCountStat()

    try:
        file_stat = os.stat(filepath)
        inode = file_stat.st_ino
        date_created = file_stat.st_ctime
        date_modified = file_stat.st_mtime
        line_count_stat = get_line_count(filepath)
    except Exception:
        error_occured = True
        error_tracebacks.append(str(traceback.format_exc()).replace("\n", ","))

    error_occured = error_occured or line_count_stat.error_occured
    error_tracebacks.append(line_count_stat.error_traceback)

    return FileStat(
        date__inode=f"{get_sqlite_datetime(scan_start_time)[:10]}__{inode}",
        date_scanned=get_sqlite_datetime(scan_start_time),
        date_modified=get_sqlite_datetime(datetime.fromtimestamp(date_modified)),
        date_created=get_sqlite_datetime(datetime.fromtimestamp(date_created)),
        lines=line_count_stat.count,
        filename=filename,
        filepath=filepath,
        error_occured=error_occured,
        error_traceback=" | ".join(error_tracebacks),
        filetype=filename.rsplit(".", 1)[1],
        inode=inode,
    )


def should_perform_scan(scan_config: ScanConfig) -> bool:
    conn, curs = get_sqlite_conn(TRACKING_TABLES["scan"].file_path)
    sql = f"""
        SELECT MAX(date_scanned) FROM {TRACKING_TABLES["scan"].table_name}
    """
    curs.execute(sql)
    data = curs.fetchall()
    perform_scan = False

    # "data[0][0] is None" condition is here because even if there are
    # no row in the table, running "MAX(date_scanned)" will return one row
    # where the value is set to None
    if not len(data) or data[0][0] is None:
        logger.debug("no previous scan detected")
        perform_scan = True
    elif datetime.now() - datetime_from_sqlite_datetime(data[0][0]) >= timedelta(
        hours=scan_config.scan_period_wait_time_hours
    ):
        logger.debug(
            f"time since last scan: {datetime.now() - datetime_from_sqlite_datetime(data[0][0])}"
        )
        perform_scan = True
    else:
        logger.debug(
            f"time since last scan: {datetime.now() - datetime_from_sqlite_datetime(data[0][0])}"
        )

    curs.close()
    conn.close()
    return perform_scan


if __name__ == "__main__":
    try:
        with open(CONFIG_PATH) as f:
            scan_config = ScanConfig.model_validate_json(f.read())

        create_tracking_tables(scan_config)

        if should_perform_scan(scan_config):
            start_time = time.time()
            files_scanned = 0
            files_skipped = 0
            current_datetime = datetime.now()

            for filepath in scan_config.scan_paths:
                logger.debug(f"scanning, root dir: {filepath}")
                file_stats: List[FileStat] = []

                for foldername, subfolders, filenames in os.walk(filepath):
                    for filename in filenames:
                        filepath = os.path.join(foldername, filename)
                        if is_tracked(filepath):
                            file_stat = collect_file_stats(
                                filepath=filepath, scan_start_time=current_datetime
                            )
                            file_stats.append(file_stat)
                            files_scanned += 1
                        else:
                            files_skipped += 1

                insert_data(TRACKING_TABLES["file"], file_stats)

            scan_time = round(time.time() - start_time, 2)
            insert_data(
                TRACKING_TABLES["scan"],
                [
                    ScanStat(
                        date_scanned=get_sqlite_datetime(current_datetime),
                        scan_time=scan_time,
                        files_scanned=files_scanned,
                        files_skipped=files_skipped,
                    )
                ],
            )

        else:
            logger.debug("skipping scan")

        dump_db_to_csv(scan_config)
        create_daily_file_report()
    except Exception:
        logger.error(str(traceback.format_exc()).replace("\n", ","))
