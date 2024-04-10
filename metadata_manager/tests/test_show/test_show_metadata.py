import pytest
import subprocess


@pytest.mark.f5a7737461
@pytest.mark.cli
@pytest.mark.show
@pytest.mark.history
@pytest.mark.sanity
def test_show_displays_metadata(working_dir, mdm, session, show_cmd):
    file_ = working_dir.joinpath("file_")
    mdm.touch(session=session, filepath=file_)
    mdm.set_value(session=session, filepath=file_, key="key_a", value="value_a")
    mdm.set_value(session=session, filepath=file_, key="key_b", value="value_b")

    output = subprocess.check_output([*show_cmd, file_, "--metadata"])

    assert "key_a" in output.decode()
    assert "value_a" in output.decode()
    assert "key_b" in output.decode()
    assert "value_b" in output.decode()
