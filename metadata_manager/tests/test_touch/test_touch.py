import pytest


@pytest.mark.c855145356
@pytest.mark.manager
@pytest.mark.touch
@pytest.mark.sanity
def test_touch_multiple_times(working_dir, mdm):
    for i in range(100):
        testfile = working_dir.joinpath(f"testfile{i}")
        mdm.touch(testfile)

    for i in range(100):
        assert working_dir.joinpath(f"testfile{i}").exists()
