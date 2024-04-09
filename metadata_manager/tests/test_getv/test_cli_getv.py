import pytest
import subprocess


@pytest.mark.bf6657689d
@pytest.mark.getv
@pytest.mark.cli
@pytest.mark.sanity
@pytest.mark.parametrize("key", ["-k", "--key"])
def test_getv_prints_value_associated_with_repository(mdm, getv_cmd, session, key):
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


@pytest.mark.af2c7652b8
@pytest.mark.getv
@pytest.mark.cli
@pytest.mark.sanity
def test_getv_gets_value_associated_with_file(getv_cmd, working_dir, mdm, session):
    file_1 = working_dir.joinpath("file_1")
    file_2 = working_dir.joinpath("file_2")
    mdm.touch(session=session, filepath=file_1)
    mdm.touch(session=session, filepath=file_2)

    mdm.set_value(session=session, filepath=file_1, key="key_a", value="value_a")
    mdm.set_value(session=session, filepath=file_1, key="key_b", value="value_b")
    mdm.set_value(session=session, filepath=file_2, key="key_c", value="value_c")

    output = subprocess.check_output([*getv_cmd, "--file", file_1, "--key", "key_a"])
    assert "value_a" in output.decode()
    assert "value_b" not in output.decode()
    assert "value_c" not in output.decode()

    output = subprocess.check_output([*getv_cmd, "--file", file_1, "--key", "key_b"])
    assert "value_a" not in output.decode()
    assert "value_b" in output.decode()
    assert "value_c" not in output.decode()

    output = subprocess.check_output([*getv_cmd, "--file", file_2, "--key", "key_c"])
    assert "value_a" not in output.decode()
    assert "value_b" not in output.decode()
    assert "value_c" in output.decode()
