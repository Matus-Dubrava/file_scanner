import pytest
from pathlib import Path
import os

from db import create_or_get_session
import tests.utils as utils


@pytest.mark.c0c0658d55
@pytest.mark.manager
@pytest.mark.sanity
def test_create_git_repository(working_dir):
    assert utils.initalize_git_repository(working_dir)
    assert (working_dir / ".git").exists()


@pytest.mark.c5dbb518a4
@pytest.mark.manager
@pytest.mark.sanity
def test_create_md_dirs(working_dir, md_manager):
    md_manager.create_md_dirs(working_dir)
    assert (working_dir / md_manager.md_config.md_dir_name).exists()
    assert (working_dir / md_manager.md_config.md_dir_name / "hashes").exists()
    assert (working_dir / md_manager.md_config.md_dir_name / "deleted").exists()


@pytest.mark.ab14fccc2b
@pytest.mark.manager
@pytest.mark.sanity
def test_dir_is_managed_by_md(working_dir, md_manager):
    # Test the path to the root, don't care about the result here
    # Just test that the function executes successfully.
    md_manager.check_dir_is_md_managed(working_dir)

    subdir1 = Path("dir1")
    subdir2 = Path("dir2")
    os.makedirs(working_dir / subdir1 / subdir2)

    assert not md_manager.check_dir_is_md_managed(
        working_dir / subdir1 / subdir2, stop_at=working_dir
    )

    md_manager.create_md_dirs(working_dir)
    assert md_manager.check_dir_is_md_managed(working_dir / subdir1 / subdir2)
    assert md_manager.check_dir_is_md_managed(working_dir / subdir1)
    assert md_manager.check_dir_is_md_managed(working_dir)


@pytest.mark.b2d798a933
@pytest.mark.manager
@pytest.mark.sanity
def test_dir_is_managed_by_md_stop_at(working_dir, md_manager):
    subdir1 = Path("dir1")
    subdir2 = Path("dir2")
    os.makedirs(working_dir / subdir1 / subdir2)

    md_manager.create_md_dirs(working_dir)

    assert not md_manager.check_dir_is_md_managed(
        working_dir / subdir1 / subdir2, stop_at=working_dir / subdir1 / subdir2
    )
    assert not md_manager.check_dir_is_md_managed(
        working_dir / subdir1 / subdir2, stop_at=working_dir / subdir1
    )
    assert md_manager.check_dir_is_md_managed(
        working_dir / subdir1 / subdir2, stop_at=working_dir
    )


@pytest.mark.fe98f4c7f9
@pytest.mark.manager
@pytest.mark.sanity
def test_dir_is_managed_by_git(working_dir, md_manager):
    # Test the path to the root, don't care about the result here
    # Just test that the function executes successfully.
    md_manager.check_dir_is_git_managed(working_dir)

    subdir1 = Path("dir1")
    subdir2 = Path("dir2")
    os.makedirs(working_dir / subdir1 / subdir2)

    assert not md_manager.check_dir_is_git_managed(
        working_dir / subdir1 / subdir2, stop_at=working_dir
    )

    assert utils.initalize_git_repository(working_dir)
    assert (working_dir / ".git").exists()

    assert md_manager.check_dir_is_git_managed(working_dir / subdir1 / subdir2)
    assert md_manager.check_dir_is_git_managed(working_dir / subdir1)
    assert md_manager.check_dir_is_git_managed(working_dir)


@pytest.mark.f670bc15f0
@pytest.mark.manager
@pytest.mark.sanity
def test_dir_is_managed_by_git_stop_at(working_dir, md_manager):
    subdir1 = Path("dir1")
    subdir2 = Path("dir2")
    os.makedirs(working_dir / subdir1 / subdir2)

    assert utils.initalize_git_repository(working_dir)

    assert not md_manager.check_dir_is_git_managed(
        working_dir / subdir1 / subdir2, stop_at=working_dir / subdir1 / subdir2
    )
    assert not md_manager.check_dir_is_git_managed(
        working_dir / subdir1 / subdir2, stop_at=working_dir / subdir1
    )
    assert md_manager.check_dir_is_git_managed(
        working_dir / subdir1 / subdir2, stop_at=working_dir
    )


@pytest.mark.f9308dd389
@pytest.mark.manager
@pytest.mark.sanity
@pytest.mark.cleanup
def test_cleanup(working_dir, md_manager):
    md_manager.create_md_dirs(working_dir)
    create_or_get_session(
        working_dir.joinpath(
            md_manager.md_config.md_dir_name, md_manager.md_config.md_db_name
        )
    )
    utils.assert_md_structure_exists(md_manager.md_config, working_dir)
    md_manager.cleanup(working_dir)
    assert not (working_dir / md_manager.md_config.md_dir_name).exists()


@pytest.mark.b8794b26cc
@pytest.mark.manager
@pytest.mark.sanity
@pytest.mark.get_md_root
def test_get_md_root_returns_none_when_no_md_dir_is_found(working_dir, md_manager):
    maybe_md_root = md_manager.get_md_root(working_dir)
    assert maybe_md_root is None


@pytest.mark.c412ccfb42
@pytest.mark.manager
@pytest.mark.sanity
@pytest.mark.get_md_root
def test_get_md_root_finds_md_repository_in_the_same_directory(working_dir, md_manager):
    md_manager.initalize_md_repository(working_dir)
    maybe_md_root = md_manager.get_md_root(working_dir)
    assert maybe_md_root == working_dir


@pytest.mark.ce52d3f1a2
@pytest.mark.manager
@pytest.mark.sanity
@pytest.mark.get_md_root
def test_get_md_root_finds_md_repository_in_parent_dir(working_dir, md_manager):
    md_manager.initalize_md_repository(working_dir)
    subdir = working_dir / "dir1" / "dir2"
    subdir.mkdir(parents=True)
    maybe_md_root = md_manager.get_md_root(working_dir)
    assert maybe_md_root == working_dir


@pytest.mark.ed354c45d0
@pytest.mark.manager
@pytest.mark.sanity
@pytest.mark.get_path_to_hash_file
def test_get_path_to_hash_file_returns_error_when_md_repository_is_not_found(
    working_dir, md_manager
):
    hashes_path_or_err = md_manager.get_path_to_hash_file(working_dir)
    assert isinstance(hashes_path_or_err, Exception)


@pytest.mark.d954542991
@pytest.mark.manager
@pytest.mark.sanity
@pytest.mark.get_path_to_hash_file
@pytest.mark.parametrize(
    "rel_filepath", ["testfile", "dir1/testfile", "dir1/dir2/testfile"]
)
def test_get_path_to_hash_file(working_dir, md_manager, rel_filepath):
    md_manager.initalize_md_repository(working_dir)
    filepath = working_dir / Path(rel_filepath)
    filepath.parent.mkdir(parents=True, exist_ok=True)
    filepath.touch()

    expected_hash_filepath = (
        working_dir / md_manager.md_config.md_dir_name / "hashes" / rel_filepath
    )

    hashes_path_or_err = md_manager.get_path_to_hash_file(filepath)
    assert hashes_path_or_err == expected_hash_filepath


@pytest.mark.f32475fd42
@pytest.mark.manager
@pytest.mark.sanity
@pytest.mark.remove_hash_file
def test_remove_hash_file_returns_error_if_md_repository_is_not_found(
    working_dir, md_manager
):
    maybe_err = md_manager.remove_hash_file(working_dir)
    assert isinstance(maybe_err, Exception)


@pytest.mark.aa367e2172
@pytest.mark.manager
@pytest.mark.sanity
@pytest.mark.remove_hash_file
@pytest.mark.parametrize(
    "rel_filepath", ["testfile", "dir1/testfile", "dir1/dir2/testfile"]
)
def test_remove_hash_file_removes_the_hash_file(working_dir, md_manager, rel_filepath):
    md_manager.initalize_md_repository(working_dir)
    filepath = working_dir / Path(rel_filepath)
    filepath.parent.mkdir(parents=True, exist_ok=True)
    filepath.touch()

    hashes_path_or_err = md_manager.get_path_to_hash_file(filepath)
    hashes_path_or_err.parent.mkdir(parents=True, exist_ok=True)

    maybe_err = md_manager.remove_hash_file(filepath)
    assert maybe_err is None
    assert not hashes_path_or_err.exists()
    # parent directory is expected to be kept
    assert hashes_path_or_err.parent.exists()


@pytest.mark.b947d7bb29
@pytest.mark.manager
@pytest.mark.sanity
@pytest.mark.remove_hash_file
def test_remove_hash_file_handles_missing_hash_file(working_dir, md_manager):
    md_manager.initalize_md_repository(working_dir)
    filepath = working_dir / "testfile"
    filepath.touch()

    maybe_err = md_manager.remove_hash_file(filepath)
    assert maybe_err is None


@pytest.mark.e000bbf3ac
@pytest.mark.manager
@pytest.mark.sanity
@pytest.mark.write_line_hashes
@pytest.mark.parametrize(
    "rel_filepath", ["testfile", "dir1/testfile", "dir1/dir2/testfile"]
)
def test_write_line_hashes_to_hash_file(working_dir, md_manager, rel_filepath):
    expected_hashes = [
        "634b027b1b69e1242d40d53e312b3b4ac7710f55be81f289b549446ef6778bee"
        "7d6fd7774f0d87624da6dcf16d0d3d104c3191e771fbe2f39c86aed4b2bf1a0f"
        "ab03c34f1ece08211fe2a8039fd6424199b3f5d7b55ff13b1134b364776c45c5"
        "63d6ff853569a0aadec5f247bba51786bb73494d1a06bdc036ebac5034a2920b"
    ]

    md_manager.initalize_md_repository(working_dir)
    filepath = working_dir / Path(rel_filepath)
    filepath.parent.mkdir(parents=True, exist_ok=True)
    filepath.touch()

    # It is expected that new hash file is created if one doesn't exist yet.
    md_manager.write_line_hashes_to_hash_file(filepath, expected_hashes[:2])
    hashes_path_or_err = md_manager.get_path_to_hash_file(filepath)

    with open(hashes_path_or_err, "r") as f:
        lines = f.readlines()
        assert expected_hashes[:2] == [line.strip() for line in lines]

    # It is expected that contents of existing hash file are overriden.
    md_manager.write_line_hashes_to_hash_file(filepath, expected_hashes[2:])

    with open(hashes_path_or_err, "r") as f:
        lines = f.readlines()
        assert expected_hashes[2:] == [line.strip() for line in lines]


@pytest.mark.c118d5efca
@pytest.mark.manager
@pytest.mark.sanity
@pytest.mark.decorator
def test_with_md_repository_paths_decorator_validation_logic(working_dir, md_manager):
    filepath = working_dir.joinpath("testfile")

    with pytest.raises(SystemExit) as err:
        md_manager.touch(filepath)

    assert err.value.code == 100


@pytest.mark.a1270e6c8b
@pytest.mark.manager
@pytest.mark.sanity
@pytest.mark.decorator
@pytest.mark.init_md(True)
def test_with_md_repository_paths_decorator_sets_paths(working_dir, md_manager):
    filepath = working_dir.joinpath("testfile")
    md_manager.touch(filepath)

    assert md_manager.repository_root == working_dir
    assert md_manager.md_path == working_dir.joinpath(md_manager.md_config.md_dir_name)
    assert md_manager.md_db_path == working_dir.joinpath(
        md_manager.md_config.md_dir_name, md_manager.md_config.md_db_name
    )
