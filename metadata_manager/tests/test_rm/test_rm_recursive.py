import pytest
import subprocess

import md_constants
from md_models import FileORM


@pytest.mark.a9b1f72f1f
@pytest.mark.cli
@pytest.mark.rm
@pytest.mark.recursive
@pytest.mark.sanity
def test_block_rm_if_directory_path_is_provided_and_recursive_flag_is_missing(
    working_dir, mdm, rm_cmd
):
    subdir = working_dir.joinpath("dir")
    subdir.mkdir()
    testfile = subdir.joinpath("testfile")

    mdm.touch(testfile)

    proc = subprocess.run([*rm_cmd, subdir], capture_output=True)
    assert proc.returncode == md_constants.MISSING_RECURSIVE_FLAG
    assert not proc.stdout
    assert "fatal:" in proc.stderr.decode().lower()

    assert testfile.exists()
    assert mdm.session.query(FileORM).filter_by(filepath=testfile).first()


@pytest.mark.d63411612c
@pytest.mark.cli
@pytest.mark.rm
@pytest.mark.recursive
@pytest.mark.sanity
def test_rm_doesnt_remove_any_file_if_recursive_flag_is_missing_and_dir_was_provided(
    working_dir, mdm, rm_cmd
):
    subdir = working_dir.joinpath("dir")
    subdir.mkdir()
    testfile1 = subdir.joinpath("testfile1")
    testfile2 = working_dir.joinpath("testfile2")

    mdm.touch(testfile1)
    mdm.touch(testfile2)

    proc = subprocess.run([*rm_cmd, testfile1, subdir], capture_output=True)
    assert proc.returncode == md_constants.MISSING_RECURSIVE_FLAG
    assert not proc.stdout
    assert "fatal:" in proc.stderr.decode().lower()

    assert testfile1.exists()
    assert testfile2.exists()
    assert mdm.session.query(FileORM).filter_by(filepath=testfile1).first()
    assert mdm.session.query(FileORM).filter_by(filepath=testfile1).first()


@pytest.mark.c4d99ee032
@pytest.mark.cli
@pytest.mark.rm
@pytest.mark.recursive
@pytest.mark.sanity
@pytest.mark.parametrize("recursive_flag", ["-r", "--recursive"])
def test_rm_removes_directory(working_dir, mdm, rm_cmd, recursive_flag):
    subdir1 = working_dir.joinpath("dir1")
    subdir2 = working_dir.joinpath("dir1", "dir2")
    subdir2.mkdir(parents=True)
    testfile = subdir2.joinpath("testfile")

    mdm.touch(testfile)

    subprocess.check_output([*rm_cmd, subdir1, recursive_flag])

    assert not subdir1.exists()
