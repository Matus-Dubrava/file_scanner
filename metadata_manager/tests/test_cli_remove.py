import pytest
import subprocess

from md_models import FileORM, HistoryORM
from md_enums import FileStatus


# test rm fails when called outside of mdm repository
@pytest.mark.a55ce6600f
@pytest.mark.cli
@pytest.mark.rm
@pytest.mark.sanity
def test_rm_fails_when_mdm_repository_doesnt_exsist(working_dir, md_manager, rm_cmd):
    filepath = working_dir.joinpath("testfile")
    filepath.touch()

    proc = subprocess.run([*rm_cmd, filepath], capture_output=True)
    assert proc.returncode == 100


@pytest.mark.a55ce6600f
@pytest.mark.cli
@pytest.mark.rm
@pytest.mark.sanity
@pytest.mark.init_md(True)
@pytest.mark.parametrize(
    "rel_filepath", ["testfile", "dir1/testfile", "dir1/dir2/testfile"]
)
def test_file_is_removed_from_both_fs_and_mdm(
    working_dir, md_manager, session, rel_filepath, rm_cmd
):
    filepath = working_dir.joinpath(rel_filepath)
    filepath.parent.mkdir(parents=True, exist_ok=True)
    md_manager.touch(filepath)
    assert filepath.exists()

    subprocess.check_output([*rm_cmd, filepath])

    # records has been renamed
    assert not session.query(FileORM).filter_by(filepath=filepath).first()
    assert not session.query(HistoryORM).filter_by(filepath=filepath).first()
    file_record = session.query(FileORM).filter_by(status=FileStatus.REMOVED).first()
    assert file_record
    history_record = (
        session.query(HistoryORM).filter_by(filepath=file_record.filepath).first()
    )
    assert history_record

    # file was removed
    assert not filepath.exists()

    # hash file was removed
    assert not md_manager.get_path_to_hash_file(filepath).exists()


@pytest.mark.edb25df6cb
@pytest.mark.cli
@pytest.mark.rm
@pytest.mark.sanity
@pytest.mark.init_md(True)
@pytest.mark.parametrize(
    "rel_filepath", ["testfile", "dir1/testfile", "dir1/dir2/testfile"]
)
def test_file_is_removed_from_fs_when_not_in_mdm(
    working_dir, md_manager, session, rel_filepath, rm_cmd
):
    filepath = working_dir.joinpath(rel_filepath)
    filepath.parent.mkdir(parents=True, exist_ok=True)
    filepath.touch()
    assert filepath.exists()

    # subprocess.check_output([*rm_cmd, filepath])
    md_manager.remove_file(filepath)

    assert not filepath.exists()
    assert not session.query(FileORM).all()
    assert not session.query(HistoryORM).all()


@pytest.mark.edb25df6cb
@pytest.mark.cli
@pytest.mark.rm
@pytest.mark.sanity
@pytest.mark.init_md(True)
@pytest.mark.parametrize(
    "rel_filepath", ["testfile", "dir1/testfile", "dir1/dir2/testfile"]
)
def test_file_is_removed_from_mdm_when_not_in_fs(
    working_dir, md_manager, session, rel_filepath, rm_cmd
):
    filepath = working_dir.joinpath(rel_filepath)
    filepath.parent.mkdir(parents=True, exist_ok=True)
    md_manager.touch(filepath)
    assert filepath.exists()

    # delete the file manually
    filepath.unlink()

    subprocess.check_output([*rm_cmd, filepath])

    assert not session.query(FileORM).filter_by(filepath=filepath).first()
    assert not session.query(HistoryORM).filter_by(filepath=filepath).first()

    file_record = session.query(FileORM).filter_by(status=FileStatus.REMOVED).first()
    assert file_record
    assert session.query(HistoryORM).filter_by(filepath=file_record.filepath).first()

    assert not md_manager.get_path_to_hash_file(filepath).exists()


@pytest.mark.a946122a8d
@pytest.mark.cli
@pytest.mark.rm
@pytest.mark.sanity
@pytest.mark.init_md(True)
def test_remove_file_not_in_fs_nor_mdm(working_dir, rm_cmd):
    filepath = working_dir.joinpath("testfile")
    subprocess.check_output([*rm_cmd, filepath])


@pytest.mark.cc524908f2
@pytest.mark.cli
@pytest.mark.rm
@pytest.mark.sanity
@pytest.mark.init_md(True)
def test_remove_file_with_purge_option(working_dir, rm_cmd, md_manager, session):
    filepath = working_dir.joinpath("testfile")
    md_manager.touch(filepath)

    subprocess.check_output([*rm_cmd, "--purge", filepath])

    assert not filepath.exists()
    assert not md_manager.get_path_to_hash_file(filepath).exists()
    assert not len(session.query(FileORM).all())
    assert not len(session.query(HistoryORM).all())


@pytest.mark.c168cc10cb
@pytest.mark.cli
@pytest.mark.rm
@pytest.mark.sanity
@pytest.mark.init_md(True)
def test_force_and_debug_flags(working_dir, rm_cmd, md_manager, session):
    filepath = working_dir.joinpath("testfile")
    md_manager.touch(filepath)

    # delete the file manualy and then instead of it create
    # a directory with the same name, that way Mdm won't be
    # able to delete it
    filepath.unlink()
    filepath.mkdir()

    # execute without flags
    proc = subprocess.run([*rm_cmd, filepath], capture_output=True)
    assert proc.returncode == 1
    assert not proc.stdout
    assert proc.stderr and "traceback" not in proc.stderr.decode().lower()
    # check that hint for user to use --force is present in stderr
    assert proc.stderr and "--force" in proc.stderr.decode().lower()

    # execute with --debug flag, expect to see traceback
    proc = subprocess.run([*rm_cmd, filepath, "--debug"], capture_output=True)
    assert proc.returncode == 1
    assert not proc.stdout
    assert proc.stderr and "traceback" in proc.stderr.decode().lower()
    # check that hint for user to use --force is present in stderr
    assert proc.stderr and "--force" in proc.stderr.decode().lower()

    # execute with --force, expect the operation to succeed
    subprocess.check_output([*rm_cmd, filepath, "--force"])

    assert session.query(FileORM).filter_by(status=FileStatus.REMOVED).first()
