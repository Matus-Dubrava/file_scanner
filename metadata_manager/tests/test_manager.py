import pytest
from pathlib import Path
import os

import tests.utils as utils
import md_utils


@pytest.mark.c0c0658d55
@pytest.mark.manager
@pytest.mark.sanity
def test_create_git_repository(working_dir):
    assert utils.initalize_git_repository(working_dir)
    assert (working_dir / ".git").exists()


@pytest.mark.c5dbb518a4
@pytest.mark.manager
@pytest.mark.sanity
def test_create_md_dirs(working_dir, mdm):
    assert (working_dir / mdm.md_config.md_dir_name).exists()
    assert (working_dir / mdm.md_config.md_dir_name / "hashes").exists()
    assert (working_dir / mdm.md_config.md_dir_name / "deleted").exists()


@pytest.mark.f9308dd389
@pytest.mark.manager
@pytest.mark.sanity
@pytest.mark.cleanup
def test_cleanup(working_dir, mdm):
    utils.assert_md_structure_exists(mdm.md_config, working_dir)
    mdm.cleanup(working_dir)
    assert not (working_dir / mdm.md_config.md_dir_name).exists()


@pytest.mark.b8794b26cc
@pytest.mark.manager
@pytest.mark.sanity
@pytest.mark.get_md_root
def test_get_md_root_returns_none_when_no_md_dir_is_found(working_dir, mdm):
    mdm.cleanup(working_dir)
    assert not md_utils.get_mdm_root(path=working_dir, config=mdm.md_config)


@pytest.mark.c412ccfb42
@pytest.mark.manager
@pytest.mark.sanity
@pytest.mark.get_md_root
def test_get_md_root_finds_md_repository_in_the_same_directory(working_dir, mdm):
    mdm_root = md_utils.get_mdm_root(path=working_dir, config=mdm.md_config)
    assert str(mdm_root) == str(working_dir)


@pytest.mark.ce52d3f1a2
@pytest.mark.manager
@pytest.mark.sanity
@pytest.mark.get_md_root
def test_get_md_root_finds_md_repository_in_parent_dir(working_dir, mdm):
    subdir = working_dir.joinpath("dir1", "dir2")
    subdir.mkdir(parents=True)
    maybe_md_root = md_utils.get_mdm_root(path=working_dir, config=mdm.md_config)
    assert maybe_md_root == working_dir


@pytest.mark.d954542991
@pytest.mark.manager
@pytest.mark.sanity
@pytest.mark.get_path_to_hash_file
@pytest.mark.parametrize(
    "rel_filepath", ["testfile", "dir1/testfile", "dir1/dir2/testfile"]
)
def test_get_path_to_hash_file(working_dir, mdm, rel_filepath):
    filepath = working_dir.joinpath(rel_filepath)
    filepath.parent.mkdir(parents=True, exist_ok=True)
    mdm.touch(filepath)

    expected_hash_filepath = working_dir.joinpath(mdm.md_path, "hashes", rel_filepath)
    hashes_path_or_err = mdm.get_path_to_hash_file(filepath)
    assert hashes_path_or_err == expected_hash_filepath


@pytest.mark.aa367e2172
@pytest.mark.manager
@pytest.mark.sanity
@pytest.mark.remove_hash_file
@pytest.mark.parametrize(
    "rel_filepath", ["testfile", "dir1/testfile", "dir1/dir2/testfile"]
)
def test_remove_hash_file_removes_the_hash_file(working_dir, mdm, rel_filepath):
    filepath = working_dir.joinpath(rel_filepath)
    filepath.parent.mkdir(parents=True, exist_ok=True)
    mdm.touch(filepath)

    hashes_path_or_err = mdm.get_path_to_hash_file(filepath)

    maybe_err = mdm.remove_hash_file(filepath)
    assert maybe_err is None
    assert not hashes_path_or_err.exists()
    # parent directory is expected to be kept
    assert hashes_path_or_err.parent.exists()


@pytest.mark.b947d7bb29
@pytest.mark.manager
@pytest.mark.sanity
@pytest.mark.remove_hash_file
def test_remove_hash_file_handles_missing_hash_file(working_dir, mdm):
    filepath = working_dir.joinpath("testfile")
    mdm.touch(filepath)

    hashes_path_or_err = mdm.get_path_to_hash_file(filepath)
    hashes_path_or_err.unlink()
    assert not hashes_path_or_err.exists()

    maybe_err = mdm.remove_hash_file(filepath)
    assert maybe_err is None


@pytest.mark.e000bbf3ac
@pytest.mark.manager
@pytest.mark.sanity
@pytest.mark.write_line_hashes
@pytest.mark.parametrize(
    "rel_filepath", ["testfile", "dir1/testfile", "dir1/dir2/testfile"]
)
def test_write_line_hashes_to_hash_file(working_dir, mdm, rel_filepath):
    expected_hashes = [
        "634b027b1b69e1242d40d53e312b3b4ac7710f55be81f289b549446ef6778bee"
        "7d6fd7774f0d87624da6dcf16d0d3d104c3191e771fbe2f39c86aed4b2bf1a0f"
        "ab03c34f1ece08211fe2a8039fd6424199b3f5d7b55ff13b1134b364776c45c5"
        "63d6ff853569a0aadec5f247bba51786bb73494d1a06bdc036ebac5034a2920b"
    ]

    filepath = working_dir.joinpath(rel_filepath)
    filepath.parent.mkdir(parents=True, exist_ok=True)
    mdm.touch(filepath)

    # It is expected that new hash file is created if one doesn't exist yet.
    mdm.write_line_hashes_to_hash_file(filepath, expected_hashes[:2])
    hashes_path_or_err = mdm.get_path_to_hash_file(filepath)

    with open(hashes_path_or_err, "r") as f:
        lines = f.readlines()
        assert expected_hashes[:2] == [line.strip() for line in lines]

    # It is expected that contents of existing hash file are overriden.
    mdm.write_line_hashes_to_hash_file(filepath, expected_hashes[2:])

    with open(hashes_path_or_err, "r") as f:
        lines = f.readlines()
        assert expected_hashes[2:] == [line.strip() for line in lines]
