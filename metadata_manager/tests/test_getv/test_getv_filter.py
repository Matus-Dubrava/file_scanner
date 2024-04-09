import pytest
import subprocess


@pytest.mark.e4bf5b29ba
@pytest.mark.filter
@pytest.mark.getv
@pytest.mark.cli
@pytest.mark.sanity
def test_getv_filters_file_records(working_dir, mdm, getv_cmd, session):
    file_1 = working_dir.joinpath("file_1")
    file_2 = working_dir.joinpath("file_2")
    file_3 = working_dir.joinpath("file_3")
    mdm.touch(session=session, filepath=file_1)
    mdm.touch(session=session, filepath=file_2)
    mdm.touch(session=session, filepath=file_3)

    mdm.set_value(
        session=session, filepath=file_1, key="common_key_123", value="common_value_123"
    )
    mdm.set_value(
        session=session, filepath=file_2, key="common_key_123", value="common_value_123"
    )
    mdm.set_value(
        session=session, filepath=file_3, key="common_key_123", value="common_value_123"
    )

    mdm.set_value(
        session=session, filepath=file_1, key="common_key_12", value="common_value_12"
    )
    mdm.set_value(
        session=session, filepath=file_2, key="common_key_12", value="common_value_12"
    )

    mdm.set_value(session=session, filepath=file_1, key="key_1", value="value_1")
    mdm.set_value(session=session, filepath=file_2, key="key_2", value="value_2")
    mdm.set_value(session=session, filepath=file_3, key="key_3", value="value_3")

    # test 'key:value" format
    output = subprocess.check_output(
        [*getv_cmd, "--filter", "common_key_123:common_value_123"]
    )
    assert file_1.name in output.decode()
    assert file_2.name in output.decode()
    assert file_3.name in output.decode()

    output = subprocess.check_output(
        [*getv_cmd, "--filter", "common_key_12:common_value_12"]
    )
    assert file_1.name in output.decode()
    assert file_2.name in output.decode()
    assert file_3.name not in output.decode()

    output = subprocess.check_output([*getv_cmd, "--filter", "key_1:value_1"])
    assert file_1.name in output.decode()
    assert file_2.name not in output.decode()
    assert file_3.name not in output.decode()

    output = subprocess.check_output([*getv_cmd, "--filter", "key_1:invalid_value"])
    assert file_1.name not in output.decode()
    assert file_2.name not in output.decode()
    assert file_3.name not in output.decode()

    output = subprocess.check_output([*getv_cmd, "--filter", "invalid_key:value_1"])
    assert file_1.name not in output.decode()
    assert file_2.name not in output.decode()
    assert file_3.name not in output.decode()

    # test 'key:' format
    output = subprocess.check_output([*getv_cmd, "--filter", "common_key_123:"])
    assert file_1.name in output.decode()
    assert file_2.name in output.decode()
    assert file_3.name in output.decode()

    output = subprocess.check_output([*getv_cmd, "--filter", "common_key_12:"])
    assert file_1.name in output.decode()
    assert file_2.name in output.decode()
    assert file_3.name not in output.decode()

    output = subprocess.check_output([*getv_cmd, "--filter", "key_1:"])
    assert file_1.name in output.decode()
    assert file_2.name not in output.decode()
    assert file_3.name not in output.decode()

    output = subprocess.check_output([*getv_cmd, "--filter", "invalid_key:"])
    assert file_1.name not in output.decode()
    assert file_2.name not in output.decode()
    assert file_3.name not in output.decode()

    # test ':value' format
    output = subprocess.check_output([*getv_cmd, "--filter", ":common_value_123"])
    assert file_1.name in output.decode()
    assert file_2.name in output.decode()
    assert file_3.name in output.decode()

    output = subprocess.check_output([*getv_cmd, "--filter", ":common_value_12"])
    assert file_1.name in output.decode()
    assert file_2.name in output.decode()
    assert file_3.name not in output.decode()

    output = subprocess.check_output([*getv_cmd, "--filter", ":value_1"])
    assert file_1.name in output.decode()
    assert file_2.name not in output.decode()
    assert file_3.name not in output.decode()

    output = subprocess.check_output([*getv_cmd, "--filter", ":invalid_value"])
    assert file_1.name not in output.decode()
    assert file_2.name not in output.decode()
    assert file_3.name not in output.decode()
