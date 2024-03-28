import pytest
import subprocess

from md_models import FileORM


@pytest.mark.e63032638e
@pytest.mark.cli
@pytest.mark.touch
@pytest.mark.parents
@pytest.mark.sanity
def test_touch_fails_if_provided_path_doesnt_exist(working_dir, touch_cmd, mdm):
    filepath = working_dir.joinpath("dir1", "testfile")

    proc = subprocess.run([*touch_cmd, filepath], capture_output=True)
    assert proc.returncode == 1
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
    working_dir, touch_cmd, parents_flag, mdm
):
    filepath = working_dir.joinpath("dir1", "testfile")
    subprocess.check_output([*touch_cmd, filepath, parents_flag])
    assert filepath.exists()

    assert mdm.session.query(FileORM).filter_by(filepath=filepath).first()
