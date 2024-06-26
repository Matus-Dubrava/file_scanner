import pytest
import subprocess

import md_enums
from models.local_models import FileORM


@pytest.mark.ede07b185a
@pytest.mark.cli
@pytest.mark.untrack
@pytest.mark.sanity
def test_untrack_fails_if_file_doesnt_exist(working_dir, untrack_cmd, mdm):
    filepath = working_dir.joinpath("testfile")
    proc = subprocess.run([*untrack_cmd, filepath], capture_output=True)
    assert proc.returncode == 1


@pytest.mark.aab6cd0a67
@pytest.mark.cli
@pytest.mark.untrack
@pytest.mark.sanity
def test_untrack_fails_if_file_is_not_in_mdm_database(working_dir, untrack_cmd, mdm):
    filepath = working_dir.joinpath("testfile")
    filepath.touch()
    proc = subprocess.run([*untrack_cmd, filepath], capture_output=True)
    assert proc.returncode == 2


@pytest.mark.ba63f21ada
@pytest.mark.cli
@pytest.mark.untrack
@pytest.mark.sanity
def test_untrack_fails_if_file_is_in_removed_state(
    working_dir, untrack_cmd, mdm, session
):
    filepath = working_dir.joinpath("testfile")
    mdm.touch(session=session, filepath=filepath)

    file_record = session.query(FileORM).filter_by(filepath=filepath).first()
    file_record.status = md_enums.FileStatus.REMOVED
    session.commit()

    proc = subprocess.run([*untrack_cmd, filepath])
    assert proc.returncode == 3


@pytest.mark.bb09a1d6d1
@pytest.mark.cli
@pytest.mark.untrack
@pytest.mark.sanity
def test_untrack_changes_file_status_to_untracked(
    working_dir, untrack_cmd, mdm, session
):
    filepath = working_dir.joinpath("testfile")
    mdm.touch(session=session, filepath=filepath)

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
def test_untrack_succeeds_if_file_is_already_untracked(
    working_dir, untrack_cmd, mdm, session
):
    filepath = working_dir.joinpath("testfile")
    mdm.touch(session=session, filepath=filepath)

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
