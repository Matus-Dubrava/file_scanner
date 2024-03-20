from typing import List, Union
from pydantic import BaseModel
import hashlib
import subprocess
from datetime import datetime

from pathlib import Path


def get_file_created_timestamp(filepath: Path) -> Union[datetime, Exception]:
    # Running "os.stat(filepath).st_ctime" doesn't return date of file creation,
    # instead it returns last date of file's metadata modification. There
    # doesn't seem to be more straightforward way to pull the file's creation
    # date on linux then this.

    # from docs (https://docs.python.org/3/library/stat.html):
    # stat.ST_CTIME
    #     The “ctime” as reported by the operating system. On some systems (like Unix)
    #     is the time of the last metadata change, and, on others (like Windows),
    #     is the creation time (see platform documentation for details).

    proc = subprocess.run(["stat", "-c", "%W", filepath], capture_output=True)

    if proc.stderr:
        return Exception(f"failed to get 'date_created' for file: {filepath}")

    timestamp = int(proc.stdout.strip())
    return datetime.fromtimestamp(timestamp)


class FileStat(BaseModel):
    n_lines: int
    hashes: List[str]
    file_hash: str


def get_line_hash(line: str) -> str:
    line_hash = hashlib.sha256()
    line_hash.update(line.encode("utf-8"))
    return line_hash.hexdigest()


def compute_file_stats(filepath: Path) -> Union[FileStat, Exception]:
    file_hash = hashlib.sha256()
    n_lines = 0
    line_hashes: List[str] = []

    try:
        with open(filepath, "r") as f:
            for line in f:
                n_lines += 1
                file_hash.update(line.encode("utf-8"))
                line_hashes.append(get_line_hash(line))
    except Exception as err:
        return err

    return FileStat(
        n_lines=n_lines, hashes=line_hashes, file_hash=file_hash.hexdigest()
    )
