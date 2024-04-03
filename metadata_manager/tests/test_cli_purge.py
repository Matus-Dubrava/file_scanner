import pytest
from md_models import FileORM, HistoryORM
from md_enums import FileStatus
import subprocess


@pytest.mark.f267590cd7
@pytest.mark.cli
@pytest.mark.sanity
@pytest.mark.purge
def test_purge_all_files_in_removed_state(working_dir, purge_cmd, mdm, session):

    filepaths = [
        working_dir.joinpath("testfile1"),
        working_dir.joinpath("testfile2"),
        working_dir.joinpath("testfile3"),
    ]

    for filepath in filepaths:
        # intentinally createing multiple history records for each file
        mdm.touch(session=session, filepath=filepath)
        mdm.touch(session=session, filepath=filepath)

    mdm.remove_files(session=session, filepaths=filepaths[1:])

    assert len(session.query(FileORM).filter_by(status=FileStatus.REMOVED).all()) == 2
    assert len(session.query(HistoryORM).all()) == 6

    subprocess.check_output([*purge_cmd, "--debug"])
    session.expire_all()

    assert len(session.query(FileORM).all()) == 1
    assert session.query(FileORM).filter_by(filepath=filepaths[0])
    assert len(session.query(HistoryORM).all()) == 2
    assert len(session.query(HistoryORM).filter_by(filepath=filepaths[0]).all()) == 2
