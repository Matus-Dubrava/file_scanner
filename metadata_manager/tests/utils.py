import subprocess
from pathlib import Path
import sqlite3

from md_models import Config


def initalize_git_repository(where: Path) -> bool:
    """
    Intialize git in target directory.
    Returns True if the operation was successful.
    """
    cmd = ["git", "init", str(where)]
    proc = subprocess.run(cmd, capture_output=True)
    return proc.returncode == 0


def assert_md_structure_exists(md_config: Config, where: Path):
    assert (where / md_config.md_dir_name).exists()
    assert (where / md_config.md_dir_name / "hashes").exists()
    assert (where / md_config.md_dir_name / "deleted").exists()
    assert (where / md_config.md_dir_name / md_config.md_db_name).exists()


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
