import pytest
import subprocess

from md_models import FileORM, HistoryORM
import md_utils
import md_enums
import utils


# Calling touch if .md repository wasn't initialized fails
@pytest.mark.e63032638e
@pytest.mark.cli
@pytest.mark.touch_subcommand
@pytest.mark.sanity
def test_touch_fails_if_provided_path_doesnt_exist(
    working_dir, touch_cmd, md_manager, initialize_working_dir, session
):
    filepath = working_dir.joinpath("dir1", "testfile")
    proc = subprocess.run([*touch_cmd, filepath], capture_output=True)
    assert proc.returncode != 0


# Calling touch directly within .md directory fails


# File doesnt exists in netiher .md nor fs
# expectations:
# - new file will be created
# - new file record will be created
# - new history record will be created
# - empty hash file will be created
@pytest.mark.b9b291b049
@pytest.mark.cli
@pytest.mark.touch_subcommand
@pytest.mark.sanity
def test_touch_creates_new_file_in_cwd(
    working_dir, touch_cmd, md_manager, initialize_working_dir, session
):
    filename = "testfile"
    proc = subprocess.run([*touch_cmd, filename], capture_output=True, cwd=working_dir)
    assert proc.returncode == 0

    file_record = session.query(FileORM).first()
    assert file_record

    history_record = (
        session.query(HistoryORM).filter_by(filepath=file_record.filepath).first()
    )
    assert history_record

    hash_filepath = md_manager.get_path_to_hash_file(working_dir.joinpath(filename))
    assert hash_filepath.exists()
    assert not len(hash_filepath.read_text())


@pytest.mark.a64ec6e711
@pytest.mark.cli
@pytest.mark.touch_subcommand
@pytest.mark.sanity
@pytest.mark.parametrize(
    "rel_filepath", ["testfile", "dir1/testfile", "dir1/dir2/testfile"]
)
def test_touch_creates_new_file_in_target_location_using_absolute_path(
    working_dir, touch_cmd, md_manager, initialize_working_dir, session, rel_filepath
):
    filepath = working_dir.joinpath(rel_filepath)
    filepath.parent.mkdir(parents=True, exist_ok=True)
    proc = subprocess.run([*touch_cmd, filepath], capture_output=True)
    assert proc.returncode == 0

    file_record = session.query(FileORM).first()
    assert file_record

    history_record = (
        session.query(HistoryORM).filter_by(filepath=file_record.filepath).first()
    )
    assert history_record

    hash_filepath = md_manager.get_path_to_hash_file(filepath)
    assert hash_filepath.exists()
    assert not len(hash_filepath.read_text())


@pytest.mark.fad5734b38
@pytest.mark.cli
@pytest.mark.touch_subcommand
@pytest.mark.sanity
@pytest.mark.parametrize(
    "rel_filepath", ["testfile", "dir1/testfile", "dir1/dir2/testfile"]
)
def test_touch_creates_new_file_in_target_location_using_relative_path(
    working_dir,
    touch_cmd,
    md_manager,
    initialize_working_dir,
    session,
    rel_filepath,
    monkeypatch,
):
    monkeypatch.chdir(working_dir)
    filepath = working_dir.joinpath(rel_filepath)
    filepath.parent.mkdir(parents=True, exist_ok=True)
    proc = subprocess.run([*touch_cmd, rel_filepath], capture_output=True)
    assert proc.returncode == 0

    file_record = session.query(FileORM).first()
    assert file_record

    history_record = (
        session.query(HistoryORM).filter_by(filepath=file_record.filepath).first()
    )
    assert history_record

    hash_filepath = md_manager.get_path_to_hash_file(filepath)
    assert hash_filepath.exists()
    assert not len(hash_filepath.read_text())


# File exists in fs but not .md
# - new file record will be created
# - new history record will be created
# - hash file will be created and populated with correct hashes
@pytest.mark.b7409e9e71
@pytest.mark.cli
@pytest.mark.touch_subcommand
@pytest.mark.sanity
def test_touch_creates_new_md_record_for_existing_file(
    working_dir, initialize_working_dir, touch_cmd, session, md_manager
):
    lines = ["test1\n", "test2\n", "test3\n"]
    expected_line_hashes = [md_utils.get_line_hash(line) for line in lines]
    filepath = working_dir.joinpath("testfile")
    with open(filepath, "w") as f:
        f.writelines(lines)

    proc = subprocess.run([*touch_cmd, filepath], capture_output=True)
    assert proc.returncode == 0

    file_record = session.query(FileORM).first()
    assert file_record

    history_record = (
        session.query(HistoryORM).filter_by(filepath=file_record.filepath).first()
    )
    assert history_record

    hash_filepath = md_manager.get_path_to_hash_file(filepath)
    assert hash_filepath.exists()

    with open(hash_filepath, "r") as f:
        actual_lines_hashes = [line.strip() for line in f.readlines()]
        assert expected_line_hashes == actual_lines_hashes


# File exits in .md but not fs
# - old .md file record and history records will be correctly updated
# - new record is added to both file and history tables
# - hash file is overwritten
@pytest.mark.f35711bc67
@pytest.mark.cli
@pytest.mark.touch_subcommand
@pytest.mark.sanity
def test_touch_handles_removed_files_cleanup(
    working_dir, initialize_working_dir, touch_cmd, session, md_manager
):
    filename = "testfile"
    filepath = working_dir.joinpath(filename)
    subprocess.check_output([*touch_cmd, filepath])
    assert filepath.exists()

    filepath.unlink()
    subprocess.check_output([*touch_cmd, filepath])
    assert filepath.exists()

    file_record = session.query(FileORM).filter_by(filepath=str(filepath)).first()
    assert file_record
    assert not file_record.timestamp_deleted
    assert file_record.status == md_enums.FileStatus.ACTIVE

    history_record = (
        session.query(HistoryORM).filter_by(filepath=file_record.filepath).first()
    )
    assert history_record

    updated_history_record = (
        session.query(FileORM)
        .filter_by(filepath=md_utils.get_filepath_with_delete_prefix(filepath))
        .first()
    )
    assert updated_history_record
    assert updated_history_record.timestamp_deleted
    assert updated_history_record.status == md_enums.FileStatus.REMOVED

    updated_history_record = (
        session.query(HistoryORM)
        .filter_by(filepath=updated_history_record.filepath)
        .first()
    )
    assert updated_history_record


# File exists in both
# expectations:
# - new history record will be created
# - file record is untouched
# - hash file is overwritten with new hash lines
# - file & history attributes are updated correctly
@pytest.mark.f4951ee374
@pytest.mark.cli
@pytest.mark.touch_subcommand
@pytest.mark.sanity
def test_each_touch_creates_new_history_record(
    working_dir, initialize_working_dir, touch_cmd, session, md_manager
):
    filepath = working_dir.joinpath("testfile")

    lines = ["a"]
    filepath.write_text("".join([f"{line}\n" for line in lines]))

    subprocess.check_output([*touch_cmd, filepath])

    file_record = session.query(FileORM).filter_by(filepath=str(filepath)).first()
    assert file_record
    assert session.query(HistoryORM).count() == 1
    latest_history_record = HistoryORM.get_latest(session)

    assert latest_history_record.count_total_lines == 1
    assert latest_history_record.count_added_lines == 1
    assert latest_history_record.count_removed_lines == 0
    assert latest_history_record.running_added_lines == 1
    assert latest_history_record.running_removed_lines == 0

    lines = ["a", "b"]
    filepath.write_text("".join([f"{line}\n" for line in lines]))

    subprocess.check_output([*touch_cmd, filepath])

    file_record = session.query(FileORM).filter_by(filepath=str(filepath)).first()
    assert session.query(HistoryORM).count() == 2
    latest_history_record = HistoryORM.get_latest(session)

    assert latest_history_record.count_total_lines == 2
    assert latest_history_record.count_added_lines == 1
    assert latest_history_record.count_removed_lines == 0
    assert latest_history_record.running_added_lines == 2
    assert latest_history_record.running_removed_lines == 0

    lines = ["a", "b", "c"]
    filepath.write_text("".join([f"{line}\n" for line in lines]))

    subprocess.check_output([*touch_cmd, filepath])

    file_record = session.query(FileORM).filter_by(filepath=str(filepath)).first()
    assert session.query(HistoryORM).count() == 3
    latest_history_record = HistoryORM.get_latest(session)

    assert latest_history_record.count_total_lines == 3
    assert latest_history_record.count_added_lines == 1
    assert latest_history_record.count_removed_lines == 0
    assert latest_history_record.running_added_lines == 3
    assert latest_history_record.running_removed_lines == 0

    lines = ["a", "b", "c"]
    filepath.write_text("".join([f"{line}\n" for line in lines]))

    subprocess.check_output([*touch_cmd, filepath])

    file_record = session.query(FileORM).filter_by(filepath=str(filepath)).first()
    assert session.query(HistoryORM).count() == 4
    latest_history_record = HistoryORM.get_latest(session)

    assert latest_history_record.count_total_lines == 3
    assert latest_history_record.count_added_lines == 0
    assert latest_history_record.count_removed_lines == 0
    assert latest_history_record.running_added_lines == 3
    assert latest_history_record.running_removed_lines == 0

    lines = ["a", "b", "d"]
    filepath.write_text("".join([f"{line}\n" for line in lines]))

    subprocess.check_output([*touch_cmd, filepath])

    file_record = session.query(FileORM).filter_by(filepath=str(filepath)).first()
    assert session.query(HistoryORM).count() == 5
    latest_history_record = HistoryORM.get_latest(session)

    assert latest_history_record.count_total_lines == 3
    assert latest_history_record.count_added_lines == 1
    assert latest_history_record.count_removed_lines == 1
    assert latest_history_record.running_added_lines == 4
    assert latest_history_record.running_removed_lines == 1

    lines = ["e"]
    filepath.write_text("".join([f"{line}\n" for line in lines]))

    subprocess.check_output([*touch_cmd, filepath])

    file_record = session.query(FileORM).filter_by(filepath=str(filepath)).first()
    assert session.query(HistoryORM).count() == 6
    latest_history_record = HistoryORM.get_latest(session)

    assert latest_history_record.count_total_lines == 1
    assert latest_history_record.count_added_lines == 1
    assert latest_history_record.count_removed_lines == 3
    assert latest_history_record.running_added_lines == 5
    assert latest_history_record.running_removed_lines == 4

    lines = []
    filepath.write_text("".join([f"{line}\n" for line in lines]))

    subprocess.check_output([*touch_cmd, filepath])

    file_record = session.query(FileORM).filter_by(filepath=str(filepath)).first()
    assert session.query(HistoryORM).count() == 7
    latest_history_record = HistoryORM.get_latest(session)

    assert latest_history_record.count_total_lines == 0
    assert latest_history_record.count_added_lines == 0
    assert latest_history_record.count_removed_lines == 1
    assert latest_history_record.running_added_lines == 5
    assert latest_history_record.running_removed_lines == 5


# Touch correctly sets version control branch name


# Touch handles multiple deletions of the same file
