import pytest
import shutil
import subprocess

import md_constants
from models.local_models import FileORM


@pytest.mark.a9b1f72f1f
@pytest.mark.cli
@pytest.mark.rm
@pytest.mark.recursive
@pytest.mark.sanity
def test_block_rm_if_directory_path_is_provided_and_recursive_flag_is_missing(
    working_dir, mdm, rm_cmd, session
):
    subdir = working_dir.joinpath("dir")
    subdir.mkdir()
    testfile = subdir.joinpath("testfile")

    mdm.touch(session=session, filepath=testfile)

    proc = subprocess.run([*rm_cmd, subdir], capture_output=True)
    assert proc.returncode == md_constants.MISSING_RECURSIVE_FLAG
    assert not proc.stdout
    assert "fatal:" in proc.stderr.decode().lower()

    assert testfile.exists()
    assert session.query(FileORM).filter_by(filepath=testfile).first()


@pytest.mark.d63411612c
@pytest.mark.cli
@pytest.mark.rm
@pytest.mark.recursive
@pytest.mark.sanity
def test_rm_doesnt_remove_any_file_if_recursive_flag_is_missing_and_dir_was_provided(
    working_dir, mdm, rm_cmd, session
):
    subdir = working_dir.joinpath("dir")
    subdir.mkdir()
    testfile1 = subdir.joinpath("testfile1")
    testfile2 = working_dir.joinpath("testfile2")

    mdm.touch(session=session, filepath=testfile1)
    mdm.touch(session=session, filepath=testfile2)

    proc = subprocess.run([*rm_cmd, testfile1, subdir], capture_output=True)
    assert proc.returncode == md_constants.MISSING_RECURSIVE_FLAG
    assert not proc.stdout
    assert "fatal:" in proc.stderr.decode().lower()

    assert testfile1.exists()
    assert testfile2.exists()
    assert session.query(FileORM).filter_by(filepath=testfile1).first()
    assert session.query(FileORM).filter_by(filepath=testfile1).first()


@pytest.mark.c4d99ee032
@pytest.mark.cli
@pytest.mark.rm
@pytest.mark.recursive
@pytest.mark.sanity
@pytest.mark.parametrize("recursive_flag", ["-r", "--recursive"])
def test_rm_removes_directory(working_dir, mdm, rm_cmd, recursive_flag, session):
    subdir1 = working_dir.joinpath("dir1")
    subdir2 = working_dir.joinpath("dir1", "dir2")
    subdir2.mkdir(parents=True)
    testfile = subdir2.joinpath("testfile")

    mdm.touch(session=session, filepath=testfile)

    subprocess.check_output([*rm_cmd, subdir1, recursive_flag])

    assert not subdir1.exists()


@pytest.mark.cbe3f3346d
@pytest.mark.cli
@pytest.mark.rm
@pytest.mark.recursive
@pytest.mark.sanity
@pytest.mark.parametrize("recursive_flag", ["-r", "--recursive"])
def test_rm_doesnt_remove_dir_containing_nontracked_files(
    working_dir, mdm, rm_cmd, recursive_flag, session
):
    """
    Scenario:
        dir1/dir2/testfile1 (tracked)
        dir1/dir2/testfile2 (not-tracked)

    Expecting 'testfile1' to be removed but 'dir1/dir2' to be preserved since
    it contains non-tracked file.
    """
    subdir1 = working_dir.joinpath("dir1")
    subdir2 = working_dir.joinpath("dir1", "dir2")
    subdir2.mkdir(parents=True)
    testfile1 = subdir2.joinpath("testfile1")
    testfile2 = subdir2.joinpath("testfile2")

    mdm.touch(session=session, filepath=testfile1)
    testfile2.touch()

    subprocess.check_output([*rm_cmd, subdir1, recursive_flag])
    assert not testfile1.exists()
    assert testfile2.exists()


@pytest.mark.e550aff5ee
@pytest.mark.cli
@pytest.mark.rm
@pytest.mark.recursive
@pytest.mark.sanity
@pytest.mark.parametrize("recursive_flag", ["-r", "--recursive"])
def test_rm_doesnt_remove_dir_containing_nontracked_files_2(
    working_dir, mdm, rm_cmd, recursive_flag, session
):
    """
    Scenario:
        dir1/dir2/testfile1 (tracked)
        dir1/testfile2 (not-tracked)

    Expecting 'testfile1' to be removed together with 'dir2' but
    'testfile2' and 'dir1' should still be present.
    """
    subdir1 = working_dir.joinpath("dir1")
    subdir2 = working_dir.joinpath("dir1", "dir2")
    subdir2.mkdir(parents=True)
    testfile1 = subdir2.joinpath("testfile1")
    testfile2 = subdir1.joinpath("testfile2")

    mdm.touch(session=session, filepath=testfile1)
    testfile2.touch()

    subprocess.check_output([*rm_cmd, subdir1, recursive_flag])
    assert not testfile1.exists()
    assert not subdir2.exists()

    assert testfile2.exists()


@pytest.mark.c4fffd06fc
@pytest.mark.cli
@pytest.mark.rm
@pytest.mark.recursive
@pytest.mark.sanity
@pytest.mark.parametrize("recursive_flag", ["-r", "--recursive"])
def test_rm_skips_dirs_no_containing_any_tracked_files(
    working_dir, mdm, rm_cmd, recursive_flag
):
    subdir1 = working_dir.joinpath("dir1")
    subdir2 = working_dir.joinpath("dir1", "dir2")
    subdir2.mkdir(parents=True)

    output = subprocess.check_output([*rm_cmd, subdir1, recursive_flag])
    assert (
        "skip:" in output.decode().lower()
        and f"{subdir1.name}" in output.decode().lower()
    )
    assert subdir1.exists() and subdir2.exists()

    output = subprocess.check_output([*rm_cmd, subdir2, recursive_flag])
    assert (
        "skip:" in output.decode().lower()
        and f"{subdir2.name}" in output.decode().lower()
    )
    assert subdir1.exists() and subdir2.exists()


@pytest.mark.a3bbd4792d
@pytest.mark.cli
@pytest.mark.rm
@pytest.mark.recursive
@pytest.mark.sanity
@pytest.mark.parametrize("recursive_flag", ["-r", "--recursive"])
def test_rm_skips_dirs_no_containing_any_tracked_files_while_deleting_other_files(
    working_dir, mdm, rm_cmd, recursive_flag, session
):
    """
    Testing scenario:
        rm testfile testdir

    where 'testdir' should be skipped but 'testfile' should be removed.
    Expecting successful deletion of the 'testfile' as well as message stating
    that 'testdir' was skipped.
    """
    subdir1 = working_dir.joinpath("dir1")
    subdir2 = working_dir.joinpath("dir1", "dir2")
    subdir2.mkdir(parents=True)

    testfile = working_dir.joinpath("testfile")
    mdm.touch(session=session, filepath=testfile)

    output = subprocess.check_output([*rm_cmd, subdir1, testfile, recursive_flag])
    assert (
        "skip:" in output.decode().lower()
        and f"{subdir1.name}" in output.decode().lower()
    )
    assert subdir1.exists() and subdir2.exists()

    assert "rm:" in output.decode().lower()
    assert not testfile.exists()


@pytest.mark.ed9d21ceb5
@pytest.mark.cli
@pytest.mark.rm
@pytest.mark.recursive
@pytest.mark.sanity
@pytest.mark.parametrize("recursive_flag", ["-r", "--recursive"])
def test_rm_removes_directory_that_is_tracked_but_doesnt_exists(
    working_dir, mdm, rm_cmd, recursive_flag, session
):
    """
    Testing scenario: directory is tracked but has been removed in the meantime (ex: using rmdir/rm -rf).
    Expecting such directory to be removed from repository.
    """
    subdir1 = working_dir.joinpath("dir1")
    subdir2 = working_dir.joinpath("dir1", "dir2")
    subdir2.mkdir(parents=True)

    testfile = subdir2.joinpath("testfile")
    mdm.touch(session=session, filepath=testfile)

    shutil.rmtree(subdir1)
    subprocess.check_output([*rm_cmd, subdir1, recursive_flag])

    assert not session.query(FileORM).filter_by(filepath=testfile).first()


@pytest.mark.eee0f42a62
@pytest.mark.cli
@pytest.mark.rm
@pytest.mark.recursive
@pytest.mark.sanity
@pytest.mark.parametrize("recursive_flag", ["-r", "--recursive"])
def test_rm_removes_empty_hashes_dirs(
    working_dir, mdm, rm_cmd, recursive_flag, session
):
    """
    Empty hash directories are deleted as well when deleting empty directories in repository.

    ex:
    (tracked) /dir/testfile
    md rm dir -r

    deletes both
    /dir
    /.md/hashes/dir
    """
    subdir = working_dir.joinpath("dir1")
    subdir.mkdir()
    testfile = subdir.joinpath("testfile")
    mdm.touch(session=session, filepath=testfile)

    subprocess.check_output([*rm_cmd, subdir, recursive_flag])

    assert not subdir.exists()
    assert not mdm.get_path_to_hash_file(filepath=subdir).exists()
