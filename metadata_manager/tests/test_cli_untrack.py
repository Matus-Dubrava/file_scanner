import pytest
import subprocess

import md_enums
from md_models import FileORM


@pytest.mark.b6c05e4f9e
@pytest.mark.cli
@pytest.mark.untrack
@pytest.mark.sanity
@pytest.mark.parametrize("rel_filepath", ["testfile", "dir1/testfile"])
def test_untrack_fails_if_file_is_outside_of_md_repository(
    working_dir, untrack_cmd, rel_filepath
):
    filepath = working_dir.joinpath(rel_filepath)
    proc = subprocess.run([*untrack_cmd, filepath], capture_output=True)
    assert proc.returncode == 100


@pytest.mark.ede07b185a
@pytest.mark.cli
@pytest.mark.untrack
@pytest.mark.sanity
@pytest.mark.init_md(True)
def test_untrack_fails_if_file_doesnt_exist(working_dir, untrack_cmd):
    filepath = working_dir.joinpath("testfile")
    proc = subprocess.run([*untrack_cmd, filepath], capture_output=True)
    assert proc.returncode == 1


@pytest.mark.aab6cd0a67
@pytest.mark.cli
@pytest.mark.untrack
@pytest.mark.sanity
@pytest.mark.init_md(True)
def test_untrack_fails_if_file_is_not_in_md_database(working_dir, untrack_cmd):
    filepath = working_dir.joinpath("testfile")
    filepath.touch()
    proc = subprocess.run([*untrack_cmd, filepath], capture_output=True)
    assert proc.returncode == 2


@pytest.mark.ba63f21ada
@pytest.mark.cli
@pytest.mark.untrack
@pytest.mark.sanity
@pytest.mark.init_md(True)
def test_untrack_fails_if_file_is_in_removed_state(
    working_dir, untrack_cmd, session, md_manager
):
    filepath = working_dir.joinpath("testfile")
    md_manager.touch(filepath)

    file_record = session.query(FileORM).filter_by(filepath=filepath).first()
    file_record.status = md_enums.FileStatus.REMOVED
    session.commit()

    proc = subprocess.run([*untrack_cmd, filepath])
    assert proc.returncode == 3


@pytest.mark.bb09a1d6d1
@pytest.mark.cli
@pytest.mark.untrack
@pytest.mark.sanity
@pytest.mark.init_md(True)
def test_untrack_changes_file_status_to_untracked(
    working_dir, untrack_cmd, session, md_manager
):
    filepath = working_dir.joinpath("testfile")
    md_manager.touch(filepath)

    file_record = session.query(FileORM).filter_by(filepath=filepath).first()
    assert file_record.status == md_enums.FileStatus.ACTIVE

    subprocess.check_output([*untrack_cmd, filepath])

    session.expire(file_record)
    file_record = session.query(FileORM).filter_by(filepath=filepath).first()
    assert file_record.status == md_enums.FileStatus.UNTRACKED


@pytest.mark.ca6cc3c2c2
@pytest.mark.cli
@pytest.mark.untrack
@pytest.mark.sanity
@pytest.mark.init_md(True)
def test_untrack_succeeds_if_file_is_already_untracked(
    working_dir, untrack_cmd, session, md_manager
):
    filepath = working_dir.joinpath("testfile")
    md_manager.touch(filepath)

    file_record = session.query(FileORM).filter_by(filepath=filepath).first()
    assert file_record.status == md_enums.FileStatus.ACTIVE

    subprocess.check_output([*untrack_cmd, filepath])

    session.expire(file_record)
    file_record = session.query(FileORM).filter_by(filepath=filepath).first()
    assert file_record.status == md_enums.FileStatus.UNTRACKED

    subprocess.check_output([*untrack_cmd, filepath])

    session.expire(file_record)
    file_record = session.query(FileORM).filter_by(filepath=filepath).first()
    assert file_record.status == md_enums.FileStatus.UNTRACKED
