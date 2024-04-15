import subprocess
import shutil
from pathlib import Path
import sqlite3

from sqlalchemy.orm import Session

from models.local_models import Config
from models.global_models import RefreshLogORM, RefreshRepositoryORM, RefreshFileORM


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


def assert_latest_refresh_repository_record(
    session: Session,
    path: Path,
    error_occured: bool,
    files_refreshed: int | None = None,
    files_failed: int | None = None,
    total_files: int | None = None,
) -> None:
    latest_refresh = RefreshLogORM.get_latest(session=session)
    assert latest_refresh, "No refresh records found."

    repo_refresh_record = (
        session.query(RefreshRepositoryORM)
        .filter_by(refresh_id=latest_refresh.id)
        .filter_by(path=path)
        .first()
    )

    assert repo_refresh_record

    assert repo_refresh_record.error_occured == 0 if not error_occured else 1
    assert repo_refresh_record.error if error_occured else not repo_refresh_record.error

    if error_occured:
        assert (
            repo_refresh_record.error_tb
            and "traceback" in repo_refresh_record.error_tb.lower()
        )
        assert files_failed is None
        assert files_refreshed is None
        assert total_files is None
    else:
        assert not repo_refresh_record.error_tb
        assert repo_refresh_record.files_failed == files_failed
        assert repo_refresh_record.files_refreshed == files_refreshed
        assert repo_refresh_record.total_files == total_files


def assert_latest_refresh_file_record(
    session: Session,
    repository_path: Path,
    filepath: Path,
    error_occured: bool,
    lines_added: int | None = None,
    lines_removed: int | None = None,
    running_lines_added: int | None = None,
    running_lines_removed: int | None = None,
):
    latest_refresh = RefreshLogORM.get_latest(session=session)
    assert latest_refresh, "No refresh records found."

    repo_refresh_record = (
        session.query(RefreshRepositoryORM)
        .filter_by(refresh_id=latest_refresh.id)
        .filter_by(path=repository_path)
        .first()
    )
    assert repo_refresh_record

    file_refresh_record = (
        session.query(RefreshFileORM)
        .filter_by(refresh_repository_id=repo_refresh_record.id)
        .filter_by(path=filepath)
        .first()
    )
    assert file_refresh_record

    if error_occured:
        assert file_refresh_record.error_occured == 1
        assert file_refresh_record.error
        assert (
            file_refresh_record.error_tb
            and "traceback" in file_refresh_record.error_tb.lower()
        )
        assert file_refresh_record.lines_added is None
        assert file_refresh_record.lines_removed is None
        assert file_refresh_record.running_lines_added is None
        assert file_refresh_record.running_lines_removed is None
    else:
        assert file_refresh_record.error_occured == 0
        assert not file_refresh_record.error
        assert not file_refresh_record.error_tb
        assert (
            file_refresh_record.lines_added == lines_added
        ), f"[lines_added] expected {lines_added}, actual {file_refresh_record.lines_added}"
        assert (
            file_refresh_record.lines_removed == lines_removed
        ), f"[lines_removed] expected {lines_removed}, actual {file_refresh_record.lines_removed}"
        assert (
            file_refresh_record.running_lines_added == running_lines_added
        ), f"[running_lines_added] expected {running_lines_added}, actual {file_refresh_record.running_lines_added}"
        assert (
            file_refresh_record.running_lines_removed == running_lines_removed
        ), f"[running_lines_removed] expected {running_lines_removed}, actual {file_refresh_record.running_lines_removed}"


def assert_count_latest_file_records(
    session: Session, count: int, repository_path: Path | None = None
) -> None:
    latest_refresh = RefreshLogORM.get_latest(session=session)
    assert latest_refresh, "No refresh records found."

    if repository_path:
        repo_refresh_record = (
            session.query(RefreshRepositoryORM)
            .filter_by(refresh_id=latest_refresh.id)
            .filter_by(path=repository_path)
            .first()
        )
        assert repo_refresh_record
        assert (
            session.query(RefreshFileORM)
            .filter_by(refresh_repository_id=repo_refresh_record.id)
            .count()
            == count
        )
    else:
        repo_refresh_ids = [
            repo_refresh.id
            for repo_refresh in (
                session.query(RefreshRepositoryORM)
                .filter_by(refresh_id=latest_refresh.id)
                .all()
            )
        ]

        actual_count = 0

        for repo_id in repo_refresh_ids:
            actual_count += (
                session.query(RefreshFileORM)
                .filter_by(refresh_repository_id=repo_id)
                .count()
            )

        assert count == actual_count


def get_latest_refresh_file_record(
    session: Session,
    repository_path: Path,
    filepath: Path,
) -> RefreshFileORM | None:
    latest_refresh = RefreshLogORM.get_latest(session=session)
    assert latest_refresh, "No refresh records found."

    repo_refresh_record = (
        session.query(RefreshRepositoryORM)
        .filter_by(refresh_id=latest_refresh.id)
        .filter_by(path=repository_path)
        .first()
    )
    assert repo_refresh_record

    return (
        session.query(RefreshFileORM)
        .filter_by(refresh_repository_id=repo_refresh_record.id)
        .filter_by(path=filepath)
        .first()
    )
