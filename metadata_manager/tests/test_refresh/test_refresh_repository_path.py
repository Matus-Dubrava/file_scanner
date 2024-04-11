import pytest
import subprocess

from models.local_models import HistoryORM


@pytest.mark.f21b5c2c5d
@pytest.mark.cli
@pytest.mark.refresh
@pytest.mark.sanity
def test_refresh_accepts_repository_path_option(working_dir, mdm, session, refresh_cmd):
    dir_ = working_dir.joinpath("dir_")
    dir_.mkdir()

    file_ = dir_.joinpath("file_")
    mdm.touch(session=session, filepath=file_)

    output = subprocess.check_output([*refresh_cmd, "--repository-path", dir_])
    assert "1" in output.decode()

    assert session.query(HistoryORM).filter_by(filepath=file_).count() == 2
