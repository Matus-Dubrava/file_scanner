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
def test_rm_fails_when_file_is_not_in_repository(working_dir, mdm, rm_cmd):
    filepath = working_dir.joinpath("testfile")
    filepath.parent.mkdir(parents=True, exist_ok=True)
    filepath.touch()
    assert filepath.exists()

    proc = subprocess.run([*rm_cmd, filepath], capture_output=True)
    assert proc.returncode == 3
    assert not proc.stdout
    assert "fatal:" in proc.stderr.decode().lower()


@pytest.mark.f5d566f944
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


# Test removing multiple files at once.


@pytest.mark.b5b55d43fe
@pytest.mark.cli
@pytest.mark.rm
@pytest.mark.sanity
def test_rm_handle_multiple_files(working_dir, rm_cmd, mdm):
    subdir = working_dir.joinpath("subdir")
    subdir.mkdir()

    testfile1 = working_dir.joinpath("testfile1")
    testfile2 = subdir.joinpath("testfile2")

    mdm.touch(testfile1)
    mdm.touch(testfile2)

    subprocess.check_output([*rm_cmd, testfile1, testfile2])
    assert not testfile1.exists()
    assert not testfile2.exists()


@pytest.mark.c168cc10cb
@pytest.mark.cli
@pytest.mark.rm
@pytest.mark.sanity
def test_rm_doesnt_remove_any_file_if_not_all_are_tracked(working_dir, rm_cmd, mdm):
    subdir = working_dir.joinpath("subdir")
    subdir.mkdir()

    testfile1 = working_dir.joinpath("testfile1")
    testfile2 = subdir.joinpath("testfile2")

    mdm.touch(testfile1)  # track testfile 1
    testfile2.touch()  # don't track testfile2

    proc = subprocess.run([*rm_cmd, testfile1, testfile2], capture_output=True)
    assert proc.returncode == 3
    assert not proc.stdout
    assert "fatal:" in proc.stderr.decode().lower()

    assert testfile1.exists()
    assert testfile2.exists()

    assert mdm.session.query(FileORM).filter_by(filepath=testfile1).first()


@pytest.mark.f429482c8b
@pytest.mark.cli
@pytest.mark.rm
@pytest.mark.sanity
def test_rm_fails_if_not_all_provided_files_belong_to_the_same_repository(
    working_dir, rm_cmd, mdm_config
):
    subdir = working_dir.joinpath("subdir")
    subdir.mkdir()

    testfile1 = working_dir.joinpath("testfile1")
    testfile2 = subdir.joinpath("testfile2")

    mdm1 = MetadataManager.new(md_config=mdm_config, path=working_dir)
    mdm2 = MetadataManager.new(md_config=mdm_config, path=subdir)

    mdm1.touch(testfile1)
    mdm2.touch(testfile2)

    # without specifing repository path
    proc = subprocess.run([*rm_cmd, testfile1, testfile2], capture_output=True)
    assert proc.returncode != 0
    assert not proc.stdout
    assert "fatal:" in proc.stderr.decode().lower()

    assert testfile1.exists()
    assert testfile2.exists()

    assert mdm1.session.query(FileORM).filter_by(filepath=testfile1).first()
    assert mdm2.session.query(FileORM).filter_by(filepath=testfile2).first()

    # with repository path
    proc = subprocess.run(
        [*rm_cmd, testfile1, testfile2, "--repository-path", working_dir],
        capture_output=True,
    )
    assert proc.returncode != 0
    assert not proc.stdout
    assert "fatal:" in proc.stderr.decode().lower()

    assert testfile1.exists()
    assert testfile2.exists()

    assert mdm1.session.query(FileORM).filter_by(filepath=testfile1).first()
    assert mdm2.session.query(FileORM).filter_by(filepath=testfile2).first()
