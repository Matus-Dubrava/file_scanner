import pytest
import subprocess

from models.local_models import HistoryORM


@pytest.mark.a635de9832
@pytest.mark.cli
@pytest.mark.refresh
@pytest.mark.sanity
def test_refresh_creates_new_history_record_for_all_tracked_files(
    working_dir, refresh_cmd, session, mdm
):
    file1 = working_dir.joinpath("file1")
    file2 = working_dir.joinpath("file2")

    mdm.touch(session=session, filepath=file1)
    mdm.touch(session=session, filepath=file2)

    subprocess.check_output([*refresh_cmd])

    assert session.query(HistoryORM).filter_by(filepath=file1).count() == 2
    assert session.query(HistoryORM).filter_by(filepath=file2).count() == 2

    file1.write_text("line1")
    file2.write_text("line1\nline2")

    subprocess.check_output([*refresh_cmd])

    assert session.query(HistoryORM).filter_by(filepath=file1).count() == 3
    assert session.query(HistoryORM).filter_by(filepath=file2).count() == 3

    # Check if line counts were updated
    assert HistoryORM.get_latest(session=session, filepath=file1).count_total_lines == 1
    assert HistoryORM.get_latest(session=session, filepath=file2).count_total_lines == 2


@pytest.mark.b209039170
@pytest.mark.cli
@pytest.mark.refresh
@pytest.mark.sanity
def test_refresh_print_number_of_processed_files(
    working_dir, refresh_cmd, session, mdm
):
    file1 = working_dir.joinpath("file1")
    file2 = working_dir.joinpath("file2")

    mdm.touch(session=session, filepath=file1)
    mdm.touch(session=session, filepath=file2)

    output = subprocess.check_output([*refresh_cmd])
    assert "2" in output.decode()


@pytest.mark.a1465ae6ee
@pytest.mark.cli
@pytest.mark.refresh
@pytest.mark.sanity
def test_refresh_updates_only_active_files(working_dir, refresh_cmd, session, mdm):
    file1 = working_dir.joinpath("file1")
    file2 = working_dir.joinpath("file2")

    mdm.touch(session=session, filepath=file1)
    mdm.touch(session=session, filepath=file2)
    mdm.remove_file(session=session, filepath=file2)

    output = subprocess.check_output([*refresh_cmd])

    assert session.query(HistoryORM).filter_by(filepath=file1).count() == 2
    assert session.query(HistoryORM).filter_by(filepath=file2).count() == 0
    assert "1" in output.decode()
