import os
import subprocess
import threading
import traceback
import warnings
from datetime import datetime
from typing import Any, List, Optional, Tuple

import apsw
import pandas as pd

import models


def get_sqlite_conn(filepath: str) -> Tuple[apsw.Connection, Any]:
    conn = apsw.Connection(filepath)
    curs = conn.cursor()
    curs.execute("PRAGMA main.journal_mode=MEMORY")
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


def pandas_read_sql_without_warnings(sql: str, conn: apsw.Connection) -> pd.DataFrame:
    # Pandas doesn't directly support apsw.Connection object but works fine
    # with it for our purposes. Therefore we are silencing the warning here.
    # silenced warning:
    #   UserWarning: pandas only supports SQLAlchemy connectable (engine/connection)
    #   or database string URI or sqlite3 DBAPI2 connection. Other DBAPI2 objects are not tested.
    #   Please consider using SQLAlchemy.
    #       df = pd.read_sql(sql, conn)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=UserWarning)
        return pd.read_sql(sql, conn)


def get_line_count(filename: str) -> models.LineCountStat:
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

    return models.LineCountStat(
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


def insert_data(table: models.TableDescription, records: List[Any]) -> None:
    sql = f"""
        INSERT INTO {table.table_name} VALUES ({table.columns_placeholder_string})
    """

    with SqliteCursorWithLock(filepath=table.file_path, lock=table.lock) as curs:
        curs.execute("begin")
        curs.executemany(sql, [record.to_tuple() for record in records])
        curs.execute("commit")


def collect_file_stats(filepath: str, scan_start_time: datetime) -> models.FileStat:
    error_occured = False
    error_tracebacks = []
    filename = os.path.basename(filepath)
    inode = 0
    date_created = 0.0
    date_modified = 0.0
    line_count_stat = models.LineCountStat()

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

    return models.FileStat(
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
