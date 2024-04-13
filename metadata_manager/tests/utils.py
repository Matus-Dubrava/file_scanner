import subprocess
import shutil
from pathlib import Path
import sqlite3

from models.local_models import Config


def initalize_git_repository(where: Path) -> bool:
    """
    Intialize git in target directory.
    Returns True if the operation was successful.
    """
    cmd = ["git", "init", str(where)]
    proc = subprocess.run(cmd, capture_output=True)
    return proc.returncode == 0


def assert_md_structure_exists(md_config: Config, where: Path):
    assert (where / md_config.local_dir_name).exists()
    assert (where / md_config.local_dir_name / "hashes").exists()
    assert (where / md_config.local_dir_name / md_config.local_db_name).exists()


def assert_database_structure(db_path: Path):
    conn = sqlite3.connect(db_path)
    curs = conn.cursor()
    sql = "SELECT name FROM sqlite_master WHERE type = 'table'"
    curs.execute(sql)
    data = curs.fetchall()

    expected_tables = [
        "file",
        "history",
        "version_info",
        "repository",
        "repository_metadata",
        "file_metadata",
    ]
    assert sorted(expected_tables) == sorted([row[0] for row in data])


def corrupt_sqlite_file(path: Path):
    """
    Corrupt sqlite file by removing the SQLite header (first 16 bytes).
    """
    tmp_db_path = Path(f"{path}_tmp")
    with open(path, "rb") as sqlite_db:
        with open(tmp_db_path, "wb") as tmp_sqlite_db:
            sqlite_db.seek(16)
            shutil.copyfileobj(sqlite_db, tmp_sqlite_db)

    shutil.copyfile(tmp_db_path, path)
    tmp_db_path.unlink()
