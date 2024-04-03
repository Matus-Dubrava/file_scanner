import pytest
import shutil
import subprocess

from md_models import FileORM, HistoryORM
import md_utils
import md_enums


@pytest.mark.da11592f36
@pytest.mark.cli
@pytest.mark.touch
@pytest.mark.sanity
def test_touch_fails_if_md_wasnt_initialized(working_dir, touch_cmd):
    filepath = working_dir.joinpath("testfile")
    proc = subprocess.run([*touch_cmd, filepath], capture_output=True)
    assert proc.returncode == 100


@pytest.mark.b9b291b049
@pytest.mark.cli
@pytest.mark.touch
@pytest.mark.sanity
def test_touch_creates_new_file_in_cwd(working_dir, touch_cmd, mdm, session):
    """
    File doesnt exists in netiher .md nor fs
    expectations:
    - new file will be created
    - new file record will be created
    - new history record will be created
    - empty hash file will be created
    """
    filename = "testfile"
    proc = subprocess.run([*touch_cmd, filename], capture_output=True, cwd=working_dir)
    assert proc.returncode == 0

    file_record = session.query(FileORM).first()
    assert file_record

    history_record = (
        session.query(HistoryORM).filter_by(filepath=file_record.filepath).first()
    )
    assert history_record

    hash_filepath = mdm.get_path_to_hash_file(working_dir.joinpath(filename))
    assert hash_filepath.exists()
    assert not len(hash_filepath.read_text())


@pytest.mark.a64ec6e711
@pytest.mark.cli
@pytest.mark.touch
@pytest.mark.sanity
@pytest.mark.parametrize(
    "rel_filepath", ["testfile", "dir1/testfile", "dir1/dir2/testfile"]
)
def test_touch_creates_new_file_in_target_location_using_absolute_path(
    working_dir, touch_cmd, mdm, rel_filepath, session
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

    hash_filepath = mdm.get_path_to_hash_file(filepath)
    assert hash_filepath.exists()
    assert not len(hash_filepath.read_text())


@pytest.mark.fad5734b38
@pytest.mark.cli
@pytest.mark.touch
@pytest.mark.sanity
@pytest.mark.parametrize(
    "rel_filepath", ["testfile", "dir1/testfile", "dir1/dir2/testfile"]
)
def test_touch_creates_new_file_in_target_location_using_relative_path(
    working_dir, touch_cmd, mdm, rel_filepath, session
):
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

    hash_filepath = mdm.get_path_to_hash_file(filepath)
    assert hash_filepath.exists()
    assert not len(hash_filepath.read_text())


@pytest.mark.b7409e9e71
@pytest.mark.cli
@pytest.mark.touch
@pytest.mark.sanity
@pytest.mark.init_md(True)
def test_touch_creates_new_md_record_for_existing_file(
    working_dir, touch_cmd, mdm, session
):
    """
    File exists in fs but not .md
    - new file record will be created
    - new history record will be created
    - hash file will be created and populated with correct hashes
    """
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

    hash_filepath = mdm.get_path_to_hash_file(filepath)
    assert hash_filepath.exists()

    with open(hash_filepath, "r") as f:
        actual_lines_hashes = [line.strip() for line in f.readlines()]
        assert expected_line_hashes == actual_lines_hashes


@pytest.mark.f35711bc67
@pytest.mark.cli
@pytest.mark.touch
@pytest.mark.sanity
def test_touch_handles_removed_files_cleanup(working_dir, touch_cmd, mdm, session):
    """
    File exits in .md but not fs,
    expecting:
    - old .md file record and history records will be correctly updated
    - new record is added to both file and history tables
    - hash file is overwritten
    """
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
        session.query(FileORM).filter_by(status=md_enums.FileStatus.REMOVED).first()
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


@pytest.mark.b41aca5376
@pytest.mark.cli
@pytest.mark.touch
@pytest.mark.sanity
def test_touch_handles_multiple_deletions_of_the_same_file(
    working_dir, touch_cmd, mdm, session
):
    filepath = working_dir.joinpath("testfile")
    subprocess.check_output([*touch_cmd, filepath])

    filepath.unlink()
    subprocess.check_output([*touch_cmd, filepath])
    assert (
        session.query(FileORM).filter_by(status=md_enums.FileStatus.ACTIVE).count() == 1
    )
    assert (
        session.query(FileORM).filter_by(status=md_enums.FileStatus.REMOVED).count()
        == 1
    )
    assert session.query(HistoryORM).filter_by(filepath=filepath).count() == 1

    filepath.unlink()
    subprocess.check_output([*touch_cmd, filepath])
    assert (
        session.query(FileORM).filter_by(status=md_enums.FileStatus.ACTIVE).count() == 1
    )
    assert (
        session.query(FileORM).filter_by(status=md_enums.FileStatus.REMOVED).count()
        == 2
    )
    assert session.query(HistoryORM).filter_by(filepath=filepath).count() == 1


@pytest.mark.f4951ee374
@pytest.mark.cli
@pytest.mark.touch
@pytest.mark.sanity
def test_each_touch_creates_new_history_record(working_dir, touch_cmd, mdm, session):
    """
    File exists in both .md and fs

    expecting:
    - new history record will be created
    - file record is untouched
    - hash file is overwritten with new hash lines
    - file & history attributes are updated correctly
    """
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
    session.expire_all()
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
    session.expire_all()
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
    session.expire_all()
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
    session.expire_all()
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
    session.expire_all()
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
    session.expire_all()
    file_record = session.query(FileORM).filter_by(filepath=str(filepath)).first()
    assert session.query(HistoryORM).count() == 7
    latest_history_record = HistoryORM.get_latest(session)

    assert latest_history_record.count_total_lines == 0
    assert latest_history_record.count_added_lines == 0
    assert latest_history_record.count_removed_lines == 1
    assert latest_history_record.running_added_lines == 5
    assert latest_history_record.running_removed_lines == 5


@pytest.mark.b2b08c4655
@pytest.mark.cli
@pytest.mark.touch
@pytest.mark.sanity
def test_init_updates_branch_name(working_dir, touch_cmd, mdm, session):
    """
    Whenever file is touched, the current branch name should be recorded
    in both 'file' record and latest 'history' record.
    """
    filepath = working_dir.joinpath("testfile")

    subprocess.check_output([*touch_cmd, filepath])
    file_record = session.query(FileORM).filter_by(filepath=filepath).first()
    history_record = HistoryORM.get_latest(session)
    assert file_record.version_control_branch is None
    assert history_record.version_control_branch is None

    initial_branch = "main"
    subprocess.check_output(["git", "init", "--initial-branch", initial_branch])

    subprocess.check_output([*touch_cmd, filepath])
    session.expire_all()
    file_record = session.query(FileORM).filter_by(filepath=filepath).first()
    history_record = HistoryORM.get_latest(session)
    assert session.query(HistoryORM).count() == 2
    assert history_record.version_control_branch == initial_branch
    assert file_record.version_control_branch == initial_branch

    new_branch_name = "develop"
    subprocess.check_output(["git", "checkout", "-b", new_branch_name])

    subprocess.check_output([*touch_cmd, filepath])
    session.expire_all()
    file_record = session.query(FileORM).filter_by(filepath=filepath).first()
    history_record = HistoryORM.get_latest(session)
    assert session.query(HistoryORM).count() == 3
    assert history_record.version_control_branch == new_branch_name
    assert file_record.version_control_branch == new_branch_name


@pytest.mark.aa43a2ae32
@pytest.mark.cli
@pytest.mark.rm
@pytest.mark.recursive
@pytest.mark.sanity
def test_touch_removes_empty_hashes_dir_when_corresponding_dir_doesnt_exist(
    working_dir, mdm, touch_cmd, session
):
    """
    Handles scenario when directories that is being tracked are removed from fs
    via other means and there is a dangling directory in hashes directory. In such case,
    if file with the same name as the dangling directory is touched, it should remove
    the dangling directory and create hash file instead.

    ex:
    (tracked) /dir/testfile
    rm -rf dir (this will lead to dangling hash dir entry)

    md touch dir (this will create new tracked file named 'dir' and remove the 'dir' directory from hashes file)
    """
    subdir = working_dir.joinpath("dir")
    subdir.mkdir()
    testfile = subdir.joinpath("testfile")
    mdm.touch(session=session, filepath=testfile)
    subprocess.check_output([*touch_cmd, testfile])

    shutil.rmtree(subdir)

    new_file = working_dir.joinpath("dir")
    subprocess.check_output([*touch_cmd, new_file])
