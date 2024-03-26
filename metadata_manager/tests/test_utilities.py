import pytest
from pathlib import Path

from md_utils import (
    count_line_changes,
    is_file_within_repository,
    get_files_belonging_to_child_repository,
    move_mdm_records,
)
from manager import MetadataManager
from md_models import FileORM, HistoryORM
from md_enums import FileStatus


@pytest.mark.e35aeef590
@pytest.mark.utils
@pytest.mark.sanity
def test_count_line_changes():
    old_hashes = ["a", "b", "c"]
    new_hashes = ["c", "b", "a"]
    assert count_line_changes(old_hashes, new_hashes).lines_added == 0
    assert count_line_changes(old_hashes, new_hashes).lines_removed == 0

    old_hashes = []
    new_hashes = ["a", "b", "c"]
    assert count_line_changes(old_hashes, new_hashes).lines_added == 3
    assert count_line_changes(old_hashes, new_hashes).lines_removed == 0

    old_hashes = ["a", "b", "c"]
    new_hashes = []
    assert count_line_changes(old_hashes, new_hashes).lines_added == 0
    assert count_line_changes(old_hashes, new_hashes).lines_removed == 3

    old_hashes = ["a", "b"]
    new_hashes = ["a", "b", "c", "d"]
    assert count_line_changes(old_hashes, new_hashes).lines_added == 2
    assert count_line_changes(old_hashes, new_hashes).lines_removed == 0

    old_hashes = ["a", "b", "c", "d"]
    new_hashes = ["a", "b"]
    assert count_line_changes(old_hashes, new_hashes).lines_added == 0
    assert count_line_changes(old_hashes, new_hashes).lines_removed == 2

    old_hashes = ["a", "b", "b"]
    new_hashes = ["a", "b", "b", "c", "c", "c"]
    assert count_line_changes(old_hashes, new_hashes).lines_added == 3
    assert count_line_changes(old_hashes, new_hashes).lines_removed == 0

    old_hashes = ["a", "b", "b", "c", "c", "c"]
    new_hashes = ["a", "b", "b"]
    assert count_line_changes(old_hashes, new_hashes).lines_added == 0
    assert count_line_changes(old_hashes, new_hashes).lines_removed == 3

    old_hashes = []
    new_hashes = []
    assert count_line_changes(old_hashes, new_hashes).lines_added == 0
    assert count_line_changes(old_hashes, new_hashes).lines_removed == 0

    old_hashes = ["a"] * 1_000_000
    new_hashes = ["a"] * 500_000 + ["b"] * 500_000
    assert count_line_changes(old_hashes, new_hashes).lines_added == 500_000
    assert count_line_changes(old_hashes, new_hashes).lines_removed == 500_000


@pytest.mark.c5e6a25207
@pytest.mark.utils
@pytest.mark.sanity
def test_is_file_within_repository():
    repository_paths = [
        Path("/"),
        Path("/dir1"),
        Path("/dir1/dir2"),
        Path("/dir1/dir2/dir3"),
        Path("/another"),
        Path("/another/dir1"),
        Path("/another/dir1/dir2"),
    ]

    filepath = Path("/testfile")
    expected_results = [True, False, False, False, False, False, False]
    for repository_path, expected_result in zip(repository_paths, expected_results):
        assert (
            is_file_within_repository(
                repository_root=repository_path, filepath=filepath
            )
            == expected_result
        )

    filepath = Path("/dir1/testfile")
    expected_results = [True, True, False, False, False, False, False]
    for repository_path, expected_result in zip(repository_paths, expected_results):
        assert (
            is_file_within_repository(
                repository_root=repository_path, filepath=filepath
            )
            == expected_result
        )

    filepath = Path("/dir1/dir2/testfile")
    expected_results = [True, True, True, False, False, False, False]
    for repository_path, expected_result in zip(repository_paths, expected_results):
        assert (
            is_file_within_repository(
                repository_root=repository_path, filepath=filepath
            )
            == expected_result
        )

    filepath = Path("/dir1/dir2/dir3/testfile")
    expected_results = [True, True, True, True, False, False, False]
    for repository_path, expected_result in zip(repository_paths, expected_results):
        assert (
            is_file_within_repository(
                repository_root=repository_path, filepath=filepath
            )
            == expected_result
        )

    filepath = Path("/dir1/dir2/dir3/dir4/testfile")
    expected_results = [True, True, True, True, False, False, False]
    for repository_path, expected_result in zip(repository_paths, expected_results):
        assert (
            is_file_within_repository(
                repository_root=repository_path, filepath=filepath
            )
            == expected_result
        )

    filepath = Path("/another/testfile")
    expected_results = [True, False, False, False, True, False, False]
    for repository_path, expected_result in zip(repository_paths, expected_results):
        assert (
            is_file_within_repository(
                repository_root=repository_path, filepath=filepath
            )
            == expected_result
        )

    filepath = Path("/another/another/testfile")
    expected_results = [True, False, False, False, True, False, False]
    for repository_path, expected_result in zip(repository_paths, expected_results):
        assert (
            is_file_within_repository(
                repository_root=repository_path, filepath=filepath
            )
            == expected_result
        )


@pytest.mark.b409ce1c42
@pytest.mark.utils
@pytest.mark.sanity
def test_get_files_belonging_to_child_repository(working_dir, mdm_config):
    dir1 = working_dir.joinpath("dir1")
    dir2 = working_dir.joinpath(dir1, "dir2")
    dir3 = working_dir.joinpath(dir2, "dir3")
    dir1.mkdir()
    dir2.mkdir()
    dir3.mkdir()

    parent_mdm = MetadataManager.new(md_config=mdm_config, path=working_dir)
    child_mdm = MetadataManager.new(md_config=mdm_config, path=dir2)

    filepath1 = working_dir.joinpath("testfile1")
    filepath2 = working_dir.joinpath(dir1, "testfile2")

    parent_mdm.touch(filepath1)
    parent_mdm.touch(filepath2)

    assert (
        get_files_belonging_to_child_repository(
            parent_mdm=parent_mdm, child_mdm=child_mdm
        )
        == []
    )

    filepath3 = working_dir.joinpath(dir2, "testfile3")
    parent_mdm.touch(filepath3)

    assert get_files_belonging_to_child_repository(
        parent_mdm=parent_mdm, child_mdm=child_mdm
    ) == [filepath3]

    filepath4 = working_dir.joinpath(dir3, "testfile4")
    parent_mdm.touch(filepath4)

    assert sorted(
        get_files_belonging_to_child_repository(
            parent_mdm=parent_mdm, child_mdm=child_mdm
        )
    ) == sorted([filepath3, filepath4])


@pytest.mark.e1e34a3c2b
@pytest.mark.utils
@pytest.mark.sanity
def test_get_files_belonging_to_child_repository_filters(working_dir, mdm_config):
    subrepository_dir = working_dir.joinpath("dir1")
    subrepository_dir.mkdir()

    active_file1 = working_dir.joinpath(subrepository_dir, "active_file1")
    active_file2 = working_dir.joinpath(subrepository_dir, "active_file2")
    untracked_file = working_dir.joinpath(subrepository_dir, "untracked_file")
    removed_file = working_dir.joinpath(subrepository_dir, "removed_file")
    subrepo_tracked_file = working_dir.joinpath(
        subrepository_dir, "subrepo_tracked_file"
    )

    parent_mdm = MetadataManager.new(md_config=mdm_config, path=working_dir)
    child_mdm = MetadataManager.new(md_config=mdm_config, path=subrepository_dir)

    parent_mdm.touch(active_file1)
    parent_mdm.touch(active_file2)
    parent_mdm.touch(untracked_file)
    parent_mdm.touch(removed_file)
    parent_mdm.touch(subrepo_tracked_file)

    parent_mdm.remove_file(removed_file)
    parent_mdm.untrack(untracked_file)

    subrepo_tracked_file_record = (
        parent_mdm.session.query(FileORM)
        .filter_by(filepath=subrepo_tracked_file)
        .first()
    )
    subrepo_tracked_file_record.status = FileStatus.TRACKED_IN_SUBREPOSITORY
    parent_mdm.session.commit()

    # Empty filters. Don't compare filepaths directly as the filepath of REMOVED files
    # are mangled. Hence the check would fail.
    filters = []
    assert (
        len(
            get_files_belonging_to_child_repository(
                parent_mdm=parent_mdm, child_mdm=child_mdm, status_filters=filters
            )
        )
        == 5
    )

    # Single value filters
    filters = [FileStatus.ACTIVE]
    assert sorted(
        get_files_belonging_to_child_repository(
            parent_mdm=parent_mdm, child_mdm=child_mdm, status_filters=filters
        )
    ) == sorted([active_file1, active_file2])

    filters = [FileStatus.REMOVED]
    result = get_files_belonging_to_child_repository(
        parent_mdm=parent_mdm, child_mdm=child_mdm, status_filters=filters
    )
    assert len(result) == 1 and str(removed_file.name) in str(result[0])

    filters = [FileStatus.UNTRACKED]
    assert sorted(
        get_files_belonging_to_child_repository(
            parent_mdm=parent_mdm, child_mdm=child_mdm, status_filters=filters
        )
    ) == sorted([untracked_file])

    filters = [FileStatus.TRACKED_IN_SUBREPOSITORY]
    assert sorted(
        get_files_belonging_to_child_repository(
            parent_mdm=parent_mdm, child_mdm=child_mdm, status_filters=filters
        )
    ) == sorted([subrepo_tracked_file])

    # Multi-valued filters
    filters = [FileStatus.ACTIVE, FileStatus.UNTRACKED]
    assert sorted(
        get_files_belonging_to_child_repository(
            parent_mdm=parent_mdm, child_mdm=child_mdm, status_filters=filters
        )
    ) == sorted([active_file1, active_file2, untracked_file])

    filters = [
        FileStatus.ACTIVE,
        FileStatus.UNTRACKED,
        FileStatus.TRACKED_IN_SUBREPOSITORY,
    ]
    assert sorted(
        get_files_belonging_to_child_repository(
            parent_mdm=parent_mdm, child_mdm=child_mdm, status_filters=filters
        )
    ) == sorted([active_file1, active_file2, untracked_file, subrepo_tracked_file])


@pytest.mark.cd476ba401
@pytest.mark.utils
@pytest.mark.sanity
def test_move_mdm_records(working_dir, mdm_config):
    another_dir = working_dir.joinpath("another")
    subrepository_dir1 = working_dir.joinpath("dir1")
    subrepository_dir2 = working_dir.joinpath(subrepository_dir1, "dir2")
    another_dir.mkdir()
    subrepository_dir1.mkdir()
    subrepository_dir2.mkdir()

    testfile1 = subrepository_dir1.joinpath("testfile1")
    testfile2 = subrepository_dir2.joinpath("testfile2")
    testfile3 = another_dir.joinpath("testfile3")

    parent_mdm = MetadataManager.new(md_config=mdm_config, path=working_dir)
    child_mdm = MetadataManager.new(md_config=mdm_config, path=subrepository_dir1)

    parent_mdm.touch(testfile1)
    parent_mdm.touch(testfile1)  # create another history record
    parent_mdm.touch(testfile2)
    parent_mdm.touch(testfile3)
    parent_mdm.untrack(testfile1)

    filepaths = get_files_belonging_to_child_repository(
        parent_mdm=parent_mdm,
        child_mdm=child_mdm,
        status_filters=[FileStatus.ACTIVE, FileStatus.UNTRACKED],
    )

    assert sorted(filepaths) == sorted([testfile1, testfile2])

    maybe_err = move_mdm_records(
        source_mdm=parent_mdm, dest_mdm=child_mdm, filepaths=filepaths
    )
    if maybe_err:
        raise maybe_err

    # Check parent file records
    assert (
        parent_mdm.session.query(FileORM).filter_by(filepath=testfile1).first().status
        == FileStatus.TRACKED_IN_SUBREPOSITORY
    )
    assert (
        parent_mdm.session.query(FileORM).filter_by(filepath=testfile2).first().status
        == FileStatus.TRACKED_IN_SUBREPOSITORY
    )
    assert (
        parent_mdm.session.query(FileORM).filter_by(filepath=testfile3).first().status
        == FileStatus.ACTIVE
    )

    # Check child file records
    assert (
        child_mdm.session.query(FileORM).filter_by(filepath=testfile1).first().status
        == FileStatus.UNTRACKED
    )
    assert (
        child_mdm.session.query(FileORM).filter_by(filepath=testfile2).first().status
        == FileStatus.ACTIVE
    )

    assert not child_mdm.session.query(FileORM).filter_by(filepath=testfile3).first()

    # Check parent history records
    assert (
        not parent_mdm.session.query(HistoryORM).filter_by(filepath=testfile1).first()
    )
    assert (
        not parent_mdm.session.query(HistoryORM).filter_by(filepath=testfile2).first()
    )
    assert (
        len(parent_mdm.session.query(HistoryORM).filter_by(filepath=testfile3).all())
        == 1
    )

    # Check child history records
    assert (
        len(child_mdm.session.query(HistoryORM).filter_by(filepath=testfile1).all())
        == 2
    )
    assert (
        len(child_mdm.session.query(HistoryORM).filter_by(filepath=testfile2).all())
        == 1
    )
    assert not child_mdm.session.query(HistoryORM).filter_by(filepath=testfile3).first()
