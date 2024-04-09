import pytest
import subprocess


@pytest.mark.d89ddc79de
@pytest.mark.all
@pytest.mark.getv
@pytest.mark.cli
@pytest.mark.sanity
@pytest.mark.parametrize("all", ["--all", "-a"])
def test_getv_prints_all_key_value_pairs_associated_with_repository(
    mdm, session, getv_cmd, all
):
    mdm.set_value(session=session, key="key_a", value="value_a")
    mdm.set_value(session=session, key="key_b", value="value_b")

    output = subprocess.check_output([*getv_cmd, all])
    assert "key_a: value_a" in output.decode()
    assert "key_b: value_b" in output.decode()


@pytest.mark.a12adf4475
@pytest.mark.all
@pytest.mark.getv
@pytest.mark.cli
@pytest.mark.sanity
@pytest.mark.parametrize("all", ["--all", "-a"])
def test_getv_prints_all_key_value_pairs_associated_with_file(
    mdm, session, getv_cmd, working_dir, all
):
    file_1 = working_dir.joinpath("file_1")
    file_2 = working_dir.joinpath("file_2")
    mdm.touch(session=session, filepath=file_1)
    mdm.touch(session=session, filepath=file_2)

    mdm.set_value(session=session, filepath=file_1, key="key_a", value="value_a")
    mdm.set_value(session=session, filepath=file_1, key="key_b", value="value_b")
    mdm.set_value(session=session, filepath=file_2, key="key_c", value="value_c")

    output = subprocess.check_output([*getv_cmd, "--file", file_1, all])
    assert "value_a" in output.decode()
    assert "value_b" in output.decode()
    assert "value_c" not in output.decode()

    output = subprocess.check_output([*getv_cmd, "--file", file_2, all])
    assert "value_a" not in output.decode()
    assert "value_b" not in output.decode()
    assert "value_c" in output.decode()
