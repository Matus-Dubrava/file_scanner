import subprocess
import pytest

from pyfakefs.fake_filesystem_unittest import Patcher


@pytest.fixture(scope="function")
def fake_filesystem(request):
    """Fixture to create a fake file system."""
    patcher = Patcher()
    patcher.setUp()
    request.addfinalizer(patcher.tearDown)
    yield patcher.fs


@pytest.mark.ebff0e4472
@pytest.mark.cli
@pytest.mark.sanity
def test_init_creates_md_repository_in_cwd(
    working_dir, init_cmd, monkeypatch, fake_filesystem
):
    fake_filesystem.create_dir("/tmp/working_dir")
    fake_filesystem.create_file("/tmp/working_dir/file.txt")

    import os
    from metadata_manager.manager import (
        create_md_dirs,
        check_dir_is_md_managed,
        is_fs_root_dir,
    )
    from pathlib import Path

    # check_dir_is_md_managed(Path("/working_dir"))

    # create_md_dirs(Path("/working_dir"))

    # print(os.mkdir("/working_dir/.md"))
    # print(os.listdir("/working_dir"))
    print(os.listdir("/home/matus"))
    proc = subprocess.run([*init_cmd, "/working_dir"], capture_output=True)
    # assert not proc.stderr
    # assert proc.returncode == 0
    # assert (working_dir / ".md").exists()

    assert 1 == 2
