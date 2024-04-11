import pytest
import subprocess

from models.local_models import FileORM, HistoryORM
from md_enums import FileStatus


@pytest.mark.db0e6365b7
@pytest.mark.cli
@pytest.mark.add
@pytest.mark.sanity
def test_add_creates_new_file_record_for_file(working_dir, add_cmd, session):
    file_ = working_dir.joinpath("file_")
    file_.touch()

    subprocess.check_output([*add_cmd, file_])

    file_record = session.query(FileORM).filter_by(filepath=file_).first()
    assert file_record.status == FileStatus.ACTIVE
    assert session.query(HistoryORM).filter_by(filepath=file_).first()


@pytest.mark.c7a63ef555
@pytest.mark.cli
@pytest.mark.add
@pytest.mark.sanity
def test_add_creates_new_file_record_for_all_files_within_directory(
    working_dir, add_cmd, session
):
    dir_ = working_dir.joinpath("dir_")
    dir_.mkdir()

    files = [
        dir_.joinpath("file1"),
        dir_.joinpath("file2"),
    ]
    for file in files:
        file.touch()

    subprocess.check_output([*add_cmd, dir_])

    for file_ in files:
        file_record = session.query(FileORM).filter_by(filepath=file_).first()
        assert file_record.status == FileStatus.ACTIVE
        assert session.query(HistoryORM).filter_by(filepath=file_).first()


@pytest.mark.accf04b988
@pytest.mark.cli
@pytest.mark.add
@pytest.mark.sanity
def test_add_handles_multiple_input_paths(working_dir, add_cmd, session):
    dir_ = working_dir.joinpath("dir_")
    dir_.mkdir()

    files = [
        dir_.joinpath("file1"),
        dir_.joinpath("file2"),
    ]
    for file in files:
        file.touch()

    file_ = working_dir.joinpath("file_")
    file_.touch()

    subprocess.check_output([*add_cmd, dir_, file_])

    for file_ in files + [file_]:
        file_record = session.query(FileORM).filter_by(filepath=file_).first()
        assert file_record.status == FileStatus.ACTIVE
        assert session.query(HistoryORM).filter_by(filepath=file_).first()


@pytest.mark.c578dccc45
@pytest.mark.cli
@pytest.mark.add
@pytest.mark.sanity
def test_add_does_nothing_if_file_is_already_tracked(
    working_dir, add_cmd, mdm, session
):
    file_ = working_dir.joinpath("file_")
    mdm.touch(session=session, filepath=file_)

    subprocess.check_output([*add_cmd, file_])

    assert session.query(HistoryORM).count() == 1


@pytest.mark.f398237ca7
@pytest.mark.cli
@pytest.mark.add
@pytest.mark.sanity
def test_add_fails_if_file_doesnt_exist(add_cmd):
    proc = subprocess.run([*add_cmd, "file_"], capture_output=True)
    assert proc.returncode != 0
    assert not proc.stdout
    assert "fatal:" in proc.stderr.decode().lower()


@pytest.mark.ee8821f5ec
@pytest.mark.cli
@pytest.mark.add
@pytest.mark.sanity
def test_add_create_hash_objects(working_dir, add_cmd, session, mdm):
    dir_ = working_dir.joinpath("dir_")
    dir_.mkdir()

    files = [
        dir_.joinpath("file1"),
        dir_.joinpath("file2"),
    ]
    for file in files:
        file.touch()

    file_ = working_dir.joinpath("file_")
    file_.touch()

    subprocess.check_output([*add_cmd, dir_, file_])

    for file_ in files + [file_]:
        hash_path = mdm.get_path_to_hash_file(file_)
        assert hash_path.exists()
