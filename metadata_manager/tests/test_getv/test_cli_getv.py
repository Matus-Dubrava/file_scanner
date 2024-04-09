import pytest
import subprocess


@pytest.mark.bf6657689d
@pytest.mark.getv
@pytest.mark.cli
@pytest.mark.sanity
@pytest.mark.parametrize("key", ["-k", "--key"])
def test_getv_prints_value_assciated_with_repository(mdm, getv_cmd, session, key):
    mdm.set_value(session=session, key="testkey", value="testvalue")
    output = subprocess.check_output([*getv_cmd, key, "testkey"])
    assert "testvalue" in output.decode()
