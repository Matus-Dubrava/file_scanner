import pytest
import subprocess


@pytest.mark.d89ddc79de
@pytest.mark.all
@pytest.mark.getv
@pytest.mark.cli
@pytest.mark.sanity
@pytest.mark.parametrize("all", ["--all", "-a"])
def test_getv_prints_all_key_value_pairs(mdm, session, getv_cmd, all):
    mdm.set_value(session=session, key="key_a", value="value_a")
    mdm.set_value(session=session, key="key_b", value="value_b")

    output = subprocess.check_output([*getv_cmd, all])
    assert "key_a: value_a" in output.decode()
    assert "key_b: value_b" in output.decode()
