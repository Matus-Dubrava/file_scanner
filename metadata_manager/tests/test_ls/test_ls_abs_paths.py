import pytest
import subprocess


@pytest.mark.b1652266cc
@pytest.mark.cli
@pytest.mark.ls
@pytest.mark.abs_paths
@pytest.mark.sanity
def test_ls_prints_filepaths_relative_to_repository_root_by_default(
    working_dir, mdm, list_cmd, session
):
    subdir = working_dir.joinpath("dir1")
    subdir.mkdir()
    testfile1 = working_dir.joinpath("testfile1")
    testfile2 = subdir.joinpath("testfile2")

    mdm.touch(session=session, filepath=testfile1)
    mdm.touch(session=session, filepath=testfile2)

    result = subprocess.check_output([*list_cmd])

    assert str(testfile1.relative_to(working_dir)) in result.decode()
    assert str(testfile2.relative_to(working_dir)) in result.decode()
    assert str(testfile1) not in result.decode()
    assert str(testfile2) not in result.decode()


@pytest.mark.fe1a15f5ed
@pytest.mark.cli
@pytest.mark.ls
@pytest.mark.abs_paths
@pytest.mark.sanity
def test_ls_prints_absolute_paths_when_abs_paths_flag_is_set(
    working_dir, mdm, list_cmd, session
):
    subdir = working_dir.joinpath("dir1")
    subdir.mkdir()
    testfile1 = working_dir.joinpath("testfile1")
    testfile2 = subdir.joinpath("testfile2")

    mdm.touch(session=session, filepath=testfile1)
    mdm.touch(session=session, filepath=testfile2)

    result = subprocess.check_output([*list_cmd, "--abs-paths"])

    assert str(testfile1) in result.decode()
    assert str(testfile2) in result.decode()
