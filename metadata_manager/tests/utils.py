import subprocess
from pathlib import Path
import sqlite3


def initalize_git_repository(where: Path) -> bool:
    """
    Intialize git in target directory.
    Returns True if the operation was successful.
    """
    cmd = ["git", "init", str(where)]
    proc = subprocess.run(cmd, capture_output=True)
    return proc.returncode == 0


def assert_md_structure_exists(where: Path):
    assert (where / ".md").exists()
    assert (where / ".md" / "hashes").exists()
    assert (where / ".md" / "deleted").exists()
    assert (where / ".md" / "metadata.db").exists()


def assert_database_structure(db_path: Path):
    conn = sqlite3.connect(db_path)
    curs = conn.cursor()
    sql = "SELECT name FROM sqlite_master where type = 'table'"
    curs.execute(sql)
    data = curs.fetchall()

    expected_tables = ["file", "history"]
    assert expected_tables == [row[0] for row in data]
