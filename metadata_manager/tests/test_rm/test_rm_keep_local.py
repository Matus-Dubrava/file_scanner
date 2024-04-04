import pytest
import subprocess

from md_models import FileORM, HistoryORM
from md_enums import FileStatus


@pytest.mark.b7a7fd9124
@pytest.mark.cli
@pytest.mark.rm
@pytest.mark.keep_local
@pytest.mark.sanity
def test_rm_keep_local_file_removes_only_db_records_and_hash_object(
    working_dir, mdm, session, rm_cmd
):
    testfile = working_dir.joinpath("testfile")
    mdm.touch(session=session, filepath=testfile)

    subprocess.check_output([*rm_cmd, testfile, "--keep-local"])
    session.expunge_all()

    assert testfile.exists()

    assert not session.query(FileORM).filter_by(filepath=testfile).first()
    assert not session.query(HistoryORM).filter_by(filepath=testfile).first()
    # without --purge, status of the file is set to REMOVED instead
    assert session.query(FileORM).filter_by(status=FileStatus.REMOVED).first()

    assert not mdm.get_path_to_hash_file(filepath=testfile).exists()


@pytest.mark.dcb20f5154
@pytest.mark.cli
@pytest.mark.rm
@pytest.mark.keep_local
@pytest.mark.sanity
def test_rm_keep_local_dir_removes_only_db_records_and_hash_object(
    working_dir, mdm, session, rm_cmd
):
    dir_ = working_dir.joinpath("dir")
    testfile = dir_.joinpath("testfile")
    mdm.touch(session=session, filepath=testfile, parents=True)

    subprocess.check_output([*rm_cmd, dir_, "--keep-local", "--recursive"])
    session.expunge_all()

    assert testfile.exists()
    assert dir_.exists()

    assert not session.query(FileORM).filter_by(filepath=testfile).first()
    assert not session.query(HistoryORM).filter_by(filepath=testfile).first()
    assert session.query(FileORM).filter_by(status=FileStatus.REMOVED).first()

    assert not mdm.get_path_to_hash_file(filepath=testfile).exists()
    assert not mdm.get_path_to_hash_file(filepath=dir_).exists()


@pytest.mark.da2a56ab0b
@pytest.mark.cli
@pytest.mark.rm
@pytest.mark.keep_local
@pytest.mark.sanity
def test_rm_keep_local_dir_removes_only_db_records_and_hash_object_with_purge(
    working_dir, mdm, session, rm_cmd
):
    dir_ = working_dir.joinpath("dir")
    testfile = dir_.joinpath("testfile")
    mdm.touch(session=session, filepath=testfile, parents=True)

    subprocess.check_output([*rm_cmd, dir_, "--keep-local", "--recursive", "--purge"])
    session.expunge_all()

    assert testfile.exists()
    assert dir_.exists()

    assert not session.query(FileORM).all()
    assert not session.query(HistoryORM).all()
