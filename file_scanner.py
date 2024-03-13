import logging
import os
import shutil
import threading
import time
import traceback
from datetime import datetime, timedelta
from logging.handlers import RotatingFileHandler
from typing import List

import apsw

import models
import utils

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.json")
LOG_TO_CONSOLE = False  # Enable for easier debugging


class Scan:
    def __init__(self, scan_config: models.ScanConfig) -> None:
        self.scan_config = scan_config

        self.tracking_tables = {
            "file": models.TableDescription(
                table_name="file",
                file_path=self.scan_config.database_filepath,
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
                csv_dump_file=os.path.join(self.scan_config.csv_dump_path, "files.csv"),
            ),
            "scan": models.TableDescription(
                table_name="scan",
                file_path=self.scan_config.database_filepath,
                lock=threading.Lock(),
                columns=[
                    ("date_scanned", "DATETIME"),
                    ("scan_time", "REAL"),
                    ("files_scanned", "INTEGER"),
                    ("files_skipped", "INTEGER"),
                ],
                primary_key=[],
                csv_dump_file=os.path.join(self.scan_config.csv_dump_path, "scans.csv"),
            ),
        }

        self.initialize_logger()
        self.initialize_dirs()
        self.intialize_tracking_tables()

    def initialize_dirs(self) -> None:
        if not os.path.exists(self.scan_config.csv_dump_path):
            os.makedirs(self.scan_config.csv_dump_path)

        if not os.path.exists(os.path.dirname(self.scan_config.log_file)):
            os.makedirs(os.path.dirname(self.scan_config.log_file))

        if not os.path.exists(self.scan_config.reports_path):
            os.makedirs(self.scan_config.reports_path)

        if not os.path.exists(os.path.dirname(self.scan_config.database_filepath)):
            os.makedirs(os.path.dirname(self.scan_config.database_filepath))

    def initialize_logger(self) -> None:
        self.logger = logging.getLogger("scan_logger")
        self.logger.setLevel(logging.DEBUG)

        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        console_handler.setFormatter(formatter)

        file_handler = RotatingFileHandler(
            self.scan_config.log_file, maxBytes=5 * 1024 * 1024, backupCount=2
        )
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)

        self.logger.addHandler(file_handler)

        if LOG_TO_CONSOLE:
            self.logger.addHandler(console_handler)

    def intialize_tracking_tables(self) -> None:
        conn, curs = utils.get_sqlite_conn(filepath=self.scan_config.database_filepath)

        for table in self.tracking_tables.values():
            sql = f"""
                CREATE TABLE IF NOT EXISTS {table.table_name} (
                    {table.columns_string}
                    {table.primary_key_string}
                )
            """
            curs.execute(sql)

        conn.close()
        curs.close()

    def dump_db_to_csv(self):
        for table in self.tracking_tables.values():
            conn, curs = utils.get_sqlite_conn(
                filepath=self.scan_config.database_filepath
            )
            sql = f"""
                SELECT * FROM {table.table_name}
            """
            curs.execute(sql)
            try:
                df = utils.pandas_read_sql_without_warnings(sql, conn)
                df.to_csv(table.csv_dump_file, index=False, header=True)
            except apsw.ExecutionCompleteError:
                # Apparently, this exception will be thrown when pandas
                # is tring to get column description (curs.description) when
                # no recrods exist in the queried table. Therefore we are skipping
                # this error.
                self.logger.debug("Skipping write to csv. No records found.")

            curs.close()
            conn.close()

    def create_daily_file_report(self):
        daily_report_total = os.path.join(
            self.scan_config.reports_path, "daily_total.csv"
        )
        daily_report_by_type = os.path.join(
            self.scan_config.reports_path, "daily_by_type.csv"
        )

        if os.path.exists(self.scan_config.reports_path):
            shutil.rmtree(self.scan_config.reports_path)

        os.makedirs(self.scan_config.reports_path)

        conn, curs = utils.get_sqlite_conn(self.tracking_tables["file"].file_path)

        sql = f"""
            SELECT
                DATE(date_modified) as date,
                filetype,
                SUM(CASE 
                    WHEN JULIANDAY(date_modified) - JULIANDAY(date_created) <= 1 THEN lines 
                    ELSE 0 END
                ) as new_lines,
                SUM(CASE 
                    WHEN JULIANDAY(date_modified) - JULIANDAY(date_created) > 1 THEN lines 
                    ELSE 0 END
                ) as lines_modified,
                SUM(lines) as total_lines,
                SUM(CASE 
                    WHEN JULIANDAY(date_modified) - JULIANDAY(date_created) <= 1 THEN 1 
                    ELSE 0 END
                ) as count_new_files,
                SUM(CASE 
                    WHEN JULIANDAY(date_modified) - JULIANDAY(date_created) > 1 THEN 1 
                    ELSE 0 END
                ) as count_files_modified,
                COUNT(*) as count_files
            FROM {self.tracking_tables["file"].table_name} as t1
            JOIN (                                              -- use this join to get only the latest record for
                                                                -- each file to avoid counting duplicates
                SELECT 
                    inode,
                    MAX(date_scanned) as max_date_scanned
                FROM {self.tracking_tables["file"].table_name}
                GROUP BY
                    inode
            ) as t2
            ON t1.inode = t2.inode AND t1.date_scanned = t2.max_date_scanned
            GROUP BY 
                DATE(date_modified),
                filetype
            ORDER BY 
                DATE(date_modified)
        """

        df = utils.pandas_read_sql_without_warnings(sql, conn)
        df.to_csv(daily_report_by_type, index=False, header=True)

        sql = f"""
            SELECT
                DATE(date_modified) as date,
                SUM(lines) as lines,
                SUM(CASE 
                    WHEN JULIANDAY(date_modified) - JULIANDAY(date_created) > 1 THEN 1 
                    ELSE 0 END
                ) as count_files_modified,
                SUM(CASE 
                    WHEN JULIANDAY(date_modified) - JULIANDAY(date_created) <= 1 THEN 1 
                    ELSE 0 END
                ) as count_new_files,
                COUNT(*) as count_files
            FROM {self.tracking_tables["file"].table_name} as t1
            JOIN (                                              -- use this join to get only the latest record for
                                                                -- each file to avoid counting duplicates
                SELECT 
                    inode,
                    MAX(date_scanned) as max_date_scanned 
                FROM {self.tracking_tables["file"].table_name}
                GROUP BY 
                    inode
            ) as t2 
            ON t1.inode = t2.inode AND t1.date_scanned = t2.max_date_scanned
            GROUP BY
                DATE(date_modified)
        """

        df = utils.pandas_read_sql_without_warnings(sql, conn)
        df.to_csv(daily_report_total, index=False, header=True)

        curs.close()
        conn.close()

    def should_perform_scan(self) -> bool:
        conn, curs = utils.get_sqlite_conn(self.tracking_tables["scan"].file_path)
        sql = f"""
            SELECT MAX(date_scanned) FROM {self.tracking_tables["scan"].table_name}
        """
        curs.execute(sql)
        data = curs.fetchall()
        perform_scan = False

        # "data[0][0] is None" condition is here because even if there are
        # no row in the table, running "MAX(date_scanned)" will return one row
        # where the value is set to None
        if not len(data) or data[0][0] is None:
            self.logger.debug("no previous scan detected")
            perform_scan = True
        elif datetime.now() - utils.datetime_from_sqlite_datetime(
            data[0][0]
        ) >= timedelta(hours=scan_config.scan_period_wait_time_hours):
            self.logger.debug(
                f"time since last scan: {datetime.now() - utils.datetime_from_sqlite_datetime(data[0][0])}"
            )
            perform_scan = True
        else:
            self.logger.debug(
                f"time since last scan: {datetime.now() - utils.datetime_from_sqlite_datetime(data[0][0])}"
            )

        curs.close()
        conn.close()
        return perform_scan


if __name__ == "__main__":
    # TODO: If something fails during Scan initialization, no logs are produced
    # since logger is part of the Scan class
    with open(CONFIG_PATH) as f:
        scan_config = models.ScanConfig.model_validate_json(f.read())

    scan = Scan(scan_config)

    try:
        if scan.should_perform_scan():
            start_time = time.time()
            files_scanned = 0
            files_skipped = 0
            current_datetime = datetime.now()

            for filepath in scan_config.scan_paths:
                scan.logger.debug(f"scanning, root dir: {filepath}")
                file_stats: List[models.FileStat] = []

                for foldername, subfolders, filenames in os.walk(filepath):
                    for filename in filenames:
                        filepath = os.path.join(foldername, filename)
                        if utils.is_tracked(filepath):
                            file_stat = utils.collect_file_stats(
                                filepath=filepath, scan_start_time=current_datetime
                            )
                            file_stats.append(file_stat)
                            files_scanned += 1
                        else:
                            files_skipped += 1

                utils.insert_data(scan.tracking_tables["file"], file_stats)

            scan_time = round(time.time() - start_time, 2)
            utils.insert_data(
                scan.tracking_tables["scan"],
                [
                    models.ScanStat(
                        date_scanned=utils.get_sqlite_datetime(current_datetime),
                        scan_time=scan_time,
                        files_scanned=files_scanned,
                        files_skipped=files_skipped,
                    )
                ],
            )

        else:
            scan.logger.debug("skipping scan")

        scan.dump_db_to_csv()
        scan.create_daily_file_report()
    except Exception:
        scan.logger.error(str(traceback.format_exc()).replace("\n", ","))
