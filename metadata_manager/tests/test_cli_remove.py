import pytest
import subprocess

from md_models import FileORM, HistoryORM
from md_enums import FileStatus
from manager import MetadataManager


# test rm fails when called outside of mdm repository
@pytest.mark.c8691b666c
@pytest.mark.cli
@pytest.mark.rm
@pytest.mark.sanity
def test_rm_fails_when_mdm_repository_doesnt_exsist(working_dir, rm_cmd):
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
def test_file_is_removed_from_both_fs_and_mdm(working_dir, mdm, rel_filepath, rm_cmd):
    filepath = working_dir.joinpath(rel_filepath)
    filepath.parent.mkdir(parents=True, exist_ok=True)
    mdm.touch(filepath)
    assert filepath.exists()

    subprocess.check_output([*rm_cmd, filepath])

    # records has been renamed
    assert not mdm.session.query(FileORM).filter_by(filepath=filepath).first()
    assert not mdm.session.query(HistoryORM).filter_by(filepath=filepath).first()
    file_record = (
        mdm.session.query(FileORM).filter_by(status=FileStatus.REMOVED).first()
    )
    assert file_record
    history_record = (
        mdm.session.query(HistoryORM).filter_by(filepath=file_record.filepath).first()
    )
    assert history_record

    # file was removed
    assert not filepath.exists()

    # hash file was removed
    assert not mdm.get_path_to_hash_file(filepath).exists()


@pytest.mark.edb25df6cb
@pytest.mark.cli
@pytest.mark.rm
@pytest.mark.sanity
@pytest.mark.parametrize(
    "rel_filepath", ["testfile", "dir1/testfile", "dir1/dir2/testfile"]
)
def test_file_is_removed_from_fs_when_not_in_mdm(working_dir, mdm, rel_filepath):
    filepath = working_dir.joinpath(rel_filepath)
    filepath.parent.mkdir(parents=True, exist_ok=True)
    filepath.touch()
    assert filepath.exists()

    # subprocess.check_output([*rm_cmd, filepath])
    mdm.remove_file(filepath)

    assert not filepath.exists()
    assert not mdm.session.query(FileORM).all()
    assert not mdm.session.query(HistoryORM).all()


@pytest.mark.edb25df6cb
@pytest.mark.cli
@pytest.mark.rm
@pytest.mark.sanity
@pytest.mark.init_md(True)
@pytest.mark.parametrize(
    "rel_filepath", ["testfile", "dir1/testfile", "dir1/dir2/testfile"]
)
def test_file_is_removed_from_mdm_when_not_in_fs(
    working_dir, mdm, rel_filepath, rm_cmd
):
    filepath = working_dir.joinpath(rel_filepath)
    filepath.parent.mkdir(parents=True, exist_ok=True)
    mdm.touch(filepath)
    assert filepath.exists()

    # delete the file manually
    filepath.unlink()

    subprocess.check_output([*rm_cmd, filepath])

    assert not mdm.session.query(FileORM).filter_by(filepath=filepath).first()
    assert not mdm.session.query(HistoryORM).filter_by(filepath=filepath).first()

    file_record = (
        mdm.session.query(FileORM).filter_by(status=FileStatus.REMOVED).first()
    )
    assert file_record
    assert (
        mdm.session.query(HistoryORM).filter_by(filepath=file_record.filepath).first()
    )

    assert not mdm.get_path_to_hash_file(filepath).exists()


@pytest.mark.a946122a8d
@pytest.mark.cli
@pytest.mark.rm
@pytest.mark.sanity
@pytest.mark.init_md(True)
def test_remove_file_not_in_fs_nor_mdm(working_dir, rm_cmd, mdm):
    filepath = working_dir.joinpath("testfile")
    subprocess.check_output([*rm_cmd, filepath])


@pytest.mark.cc524908f2
@pytest.mark.cli
@pytest.mark.rm
@pytest.mark.sanity
def test_remove_file_with_purge_option(working_dir, rm_cmd, mdm):
    filepath = working_dir.joinpath("testfile")
    mdm.touch(filepath)

    subprocess.check_output([*rm_cmd, "--purge", filepath])

    assert not filepath.exists()
    assert not mdm.get_path_to_hash_file(filepath).exists()
    assert not len(mdm.session.query(FileORM).all())
    assert not len(mdm.session.query(HistoryORM).all())


@pytest.mark.c168cc10cb
@pytest.mark.cli
@pytest.mark.rm
@pytest.mark.sanity
def test_force_and_debug_flags(working_dir, rm_cmd, mdm):
    filepath = working_dir.joinpath("testfile")
    mdm.touch(filepath)

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

    assert mdm.session.query(FileORM).filter_by(status=FileStatus.REMOVED).first()


@pytest.mark.e26c08024c
@pytest.mark.cli
@pytest.mark.rm
@pytest.mark.sanity
def test_rm_cant_be_used_outside_of_mdm_repository(
    working_dir, rm_cmd, mdm_config, monkeypatch
):
    repository_root = working_dir.joinpath("dir1")
    repository_root.mkdir()
    subdir = repository_root.joinpath("dir2")
    subdir.mkdir()

    filepath1 = repository_root.joinpath("testfile1")
    filepath2 = subdir.joinpath("testfile2")

    mdm = MetadataManager.new(md_config=mdm_config, path=repository_root)
    mdm.touch(filepath1)
    mdm.touch(filepath2)

    # These operations should fail since we are switching away from mdm repository.
    monkeypatch.chdir(working_dir)
    proc = subprocess.run([*rm_cmd, filepath1], capture_output=True)
    assert proc.returncode == 100
    proc = subprocess.run([*rm_cmd, filepath2], capture_output=True)
    assert proc.returncode == 100
    assert filepath1.exists()
    assert filepath2.exists()

    # Now they should work correctly.
    monkeypatch.chdir(repository_root)
    subprocess.check_output([*rm_cmd, filepath1])
    subprocess.check_output([*rm_cmd, filepath2])
    assert not filepath1.exists()
    assert not filepath2.exists()


# test rming multiple files at once

# test rming glob pattern
