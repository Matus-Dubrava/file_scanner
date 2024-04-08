import pytest
import subprocess


@pytest.mark.b6d335e0f1
@pytest.mark.cli
@pytest.mark.refresh
@pytest.mark.verbose
@pytest.mark.parametrize("verbose_flag", ["-v", "--verbose"])
def test_refresh_prints_verbose_information(
    working_dir, refresh_cmd, mdm, session, verbose_flag
):
    file1 = working_dir.joinpath("file1")
    file2 = working_dir.joinpath("file2")

    mdm.touch(session=session, filepath=file1)
    mdm.touch(session=session, filepath=file2)

    output = subprocess.check_output([*refresh_cmd, verbose_flag])

    assert "file1" in output.decode().lower()
    assert "file2" in output.decode().lower()
