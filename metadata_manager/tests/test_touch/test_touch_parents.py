import pytest
import subprocess

from md_models import FileORM
import md_constants


@pytest.mark.e63032638e
@pytest.mark.cli
@pytest.mark.touch
@pytest.mark.parents
@pytest.mark.sanity
def test_touch_fails_if_provided_path_doesnt_exist(working_dir, touch_cmd):
    filepath = working_dir.joinpath("dir1", "testfile")

    proc = subprocess.run([*touch_cmd, filepath], capture_output=True)
    assert proc.returncode == md_constants.NOT_MDM_REPOSITORY
    assert not proc.stdout
    assert "traceback" not in proc.stderr.decode().lower()
    assert "fatal" in proc.stderr.decode().lower()


@pytest.mark.e38b848c62
@pytest.mark.cli
@pytest.mark.touch
@pytest.mark.parents
@pytest.mark.sanity
@pytest.mark.parametrize("parents_flag", ["-p", "--parents"])
def test_touch_create_parent_directories_when_parents_flag_is_provided(
    working_dir, touch_cmd, parents_flag, session
):
    filepath = working_dir.joinpath("dir1", "testfile")
    subprocess.check_output([*touch_cmd, filepath, parents_flag])
    assert filepath.exists()

    assert session.query(FileORM).filter_by(filepath=filepath).first()


@pytest.mark.ec6fc782c2
@pytest.mark.cli
@pytest.mark.touch
@pytest.mark.parents
@pytest.mark.sanity
def test_touch_doesnt_create_any_file_without_parents_flag(
    working_dir, touch_cmd, session
):
    """
    If multiple files are provided at once, if any of those files cannot be created because
    of missing parent directory and '--parents' flag wasn't provided, no file should be created.
    """
    testfiles = [
        working_dir.joinpath("testfile1"),
        working_dir.joinpath("testfile2"),
        working_dir.joinpath("dir/testfile3"),  # 'dir' doesn't exist.
    ]

    proc = subprocess.run([*touch_cmd, *testfiles], capture_output=True)
    assert not proc.stdout
    assert proc.returncode != 0

    for testfile in testfiles:
        assert not testfile.exists()
        assert not session.query(FileORM).filter_by(filepath=testfile).first()


@pytest.mark.e5664ac516
@pytest.mark.cli
@pytest.mark.touch
@pytest.mark.parents
@pytest.mark.sanity
@pytest.mark.parametrize("parents_flag", ["-p", "--parents"])
def test_touch_creates_all_files_when_parent_flag_is_provided(
    working_dir, touch_cmd, session, parents_flag
):
    testfiles = [
        working_dir.joinpath("testfile1"),
        working_dir.joinpath("testfile2"),
        working_dir.joinpath("dir/testfile3"),  # 'dir' doesn't exist.
    ]

    subprocess.check_output([*touch_cmd, *testfiles, parents_flag])

    for testfile in testfiles:
        assert testfile.exists()
        assert session.query(FileORM).filter_by(filepath=testfile).first()
