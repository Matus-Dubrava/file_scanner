import pytest
from md_models import FileORM, HistoryORM, FileMetadataORM
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


@pytest.mark.b9fecb1939
@pytest.mark.cli
@pytest.mark.sanity
@pytest.mark.purge
def test_purge_all_metadata_in_removed_state(working_dir, purge_cmd, mdm, session):
    file_1 = working_dir.joinpath("file_1")
    file_2 = working_dir.joinpath("file_2")
    file_3 = working_dir.joinpath("file_3")

    mdm.touch(session=session, filepath=file_1)
    mdm.touch(session=session, filepath=file_2)
    mdm.touch(session=session, filepath=file_3)

    mdm.set_value(session=session, filepath=file_1, key="key_a", value="value_a")
    mdm.set_value(session=session, filepath=file_1, key="key_b", value="value_b")
    mdm.set_value(session=session, filepath=file_2, key="key_c", value="value_c")
    mdm.set_value(session=session, filepath=file_3, key="key_d", value="value_d")

    mdm.remove_file(session=session, filepath=file_1)
    mdm.remove_file(session=session, filepath=file_2)

    subprocess.check_output([*purge_cmd])

    assert not session.query(FileMetadataORM).filter_by(key="key_a").first()
    assert not session.query(FileMetadataORM).filter_by(key="key_b").first()
    assert not session.query(FileMetadataORM).filter_by(key="key_c").first()
    assert session.query(FileMetadataORM).filter_by(key="key_d").first()
