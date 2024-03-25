import pytest
from md_models import FileORM, HistoryORM
from md_enums import FileStatus
import subprocess


@pytest.mark.f267590cd7
@pytest.mark.cli
@pytest.mark.sanity
@pytest.mark.purge
def test_purge_all_files_in_removed_state(working_dir, purge_cmd, mdm):

    filepaths = [
        working_dir.joinpath("testfile1"),
        working_dir.joinpath("testfile2"),
        working_dir.joinpath("testfile3"),
    ]

    for filepath in filepaths:
        # intentinally createing multiple history records for each file
        mdm.touch(filepath)
        mdm.touch(filepath)

    mdm.remove_file(filepaths[1])
    mdm.remove_file(filepaths[2])

    assert (
        len(mdm.session.query(FileORM).filter_by(status=FileStatus.REMOVED).all()) == 2
    )
    assert len(mdm.session.query(HistoryORM).all()) == 6

    subprocess.check_output([*purge_cmd])
    mdm.session.expire_all()

    assert len(mdm.session.query(FileORM).all()) == 1
    assert mdm.session.query(FileORM).filter_by(filepath=filepaths[0])
    assert len(mdm.session.query(HistoryORM).all()) == 2
    assert (
        len(mdm.session.query(HistoryORM).filter_by(filepath=filepaths[0]).all()) == 2
    )
