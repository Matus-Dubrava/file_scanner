import pytest
from pathlib import Path

from md_utils import (
    count_line_changes,
    is_file_within_repository,
    get_files_belonging_to_child_repository,
)
from manager import MetadataManager


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
