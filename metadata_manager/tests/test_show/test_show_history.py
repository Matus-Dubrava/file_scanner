import pytest
import subprocess


@pytest.mark.c5591b611f
@pytest.mark.cli
@pytest.mark.show
@pytest.mark.history
@pytest.mark.sanity
def test_show_displays_file_history(working_dir, mdm, session, show_cmd):
    file_ = working_dir.joinpath("file_")
    mdm.touch(session=session, filepath=file_)
    mdm.touch(session=session, filepath=file_)
    mdm.touch(session=session, filepath=file_)

    output = subprocess.check_output([*show_cmd, file_, "--history"])
    assert output.decode().lower().count("recorded") == 3


@pytest.mark.b5a9c04c05
@pytest.mark.cli
@pytest.mark.show
@pytest.mark.history
@pytest.mark.sanity
def test_show_displays_limited_history(working_dir, mdm, session, show_cmd):
    file_ = working_dir.joinpath("file_")
    mdm.touch(session=session, filepath=file_)
    mdm.touch(session=session, filepath=file_)
    mdm.touch(session=session, filepath=file_)

    output = subprocess.check_output([*show_cmd, file_, "--history", "-n", "2"])
    assert output.decode().lower().count("recorded") == 2
