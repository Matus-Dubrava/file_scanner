import pytest
from pathlib import Path
import subprocess

from md_models import FileORM, HistoryORM

# Calling touch if .md repository wasn't initialized fails

# Calling touch directly within .md directory fails


# File doesnt exists in netiher .md nor fs
# expectations:
# - new file will be created
# - new file record will be created
# - new history record will be created
# - empty hash file will be created
@pytest.mark.b9b291b049
@pytest.mark.cli
@pytest.mark.touch_subcommand
@pytest.mark.sanity
def test_touch_creates_new_file_in_cwd(
    working_dir, touch_cmd, md_manager, initialize_working_dir, session
):
    filename = "testfile"
    proc = subprocess.run([*touch_cmd, filename], capture_output=True, cwd=working_dir)
    assert proc.returncode == 0

    file_record = session.query(FileORM).first()
    assert file_record

    history_record = (
        session.query(HistoryORM)
        .filter_by(filepath__git_branch=file_record.filepath__git_branch)
        .first()
    )
    assert history_record

    assert md_manager.get_path_to_hash_file(working_dir.joinpath(filename)).exists()


@pytest.mark.a64ec6e711
@pytest.mark.cli
@pytest.mark.touch_subcommand
@pytest.mark.sanity
@pytest.mark.parametrize(
    "rel_filepath", ["testfile", "dir1/testfile", "dir1/dir2/testfile"]
)
def test_touch_creates_new_file_in_target_location_using_absolute_path(
    working_dir, touch_cmd, md_manager, initialize_working_dir, session, rel_filepath
):
    filepath = working_dir.joinpath(rel_filepath)
    filepath.parent.mkdir(parents=True, exist_ok=True)
    proc = subprocess.run([*touch_cmd, filepath], capture_output=True)
    assert proc.returncode == 0

    file_record = session.query(FileORM).first()
    assert file_record

    history_record = (
        session.query(HistoryORM)
        .filter_by(filepath__git_branch=file_record.filepath__git_branch)
        .first()
    )
    assert history_record

    assert md_manager.get_path_to_hash_file(filepath).exists()


@pytest.mark.fad5734b38
@pytest.mark.cli
@pytest.mark.touch_subcommand
@pytest.mark.sanity
@pytest.mark.parametrize(
    "rel_filepath", ["testfile", "dir1/testfile", "dir1/dir2/testfile"]
)
def test_touch_creates_new_file_in_target_location_using_relative_path(
    working_dir,
    touch_cmd,
    md_manager,
    initialize_working_dir,
    session,
    rel_filepath,
    monkeypatch,
):
    monkeypatch.chdir(working_dir)
    filepath = working_dir.joinpath(rel_filepath)
    filepath.parent.mkdir(parents=True, exist_ok=True)
    proc = subprocess.run([*touch_cmd, rel_filepath], capture_output=True)
    assert proc.returncode == 0

    file_record = session.query(FileORM).first()
    assert file_record

    history_record = (
        session.query(HistoryORM)
        .filter_by(filepath__git_branch=file_record.filepath__git_branch)
        .first()
    )
    assert history_record

    assert md_manager.get_path_to_hash_file(filepath).exists()


# File exists in fs but not .md
# - new file record will be created
# - new history record will be created
# - hash file will be created and populated with correct hashes

# File exits in .md but not fs
# - old .md file record and history records will be correctly updated
# - new record is added to both file and history tables
# - hash file is overwritten


# File exists in both
# expectations:
# - new history record will be created
# - file record is untouched
# - hash file is overwritten with new hash lines
