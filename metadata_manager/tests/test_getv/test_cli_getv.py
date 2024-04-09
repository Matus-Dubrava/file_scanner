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


@pytest.mark.ad052f9015
@pytest.mark.getv
@pytest.mark.cli
@pytest.mark.sanity
@pytest.mark.parametrize("key", ["-k", "--key"])
@pytest.mark.parametrize("all", ["-a", "--all"])
def test_getv_requires_one_of_all_or_key_to_be_specified_but_not_both(
    getv_cmd, key, all
):
    proc = subprocess.run([*getv_cmd], capture_output=True)
    assert proc.returncode != 0
    assert not proc.stdout
    assert "fatal:" in proc.stderr.decode().lower()

    proc = subprocess.run([*getv_cmd, key, "somekey", all], capture_output=True)
    assert proc.returncode != 0
    assert not proc.stdout
    assert "fatal:" in proc.stderr.decode().lower()
