import pytest
from pathlib import Path
import os

from metadata_manager.manager import (
    check_dir_is_md_managed,
    create_md_dirs,
    check_dir_is_git_managed,
    cleanup,
)
from metadata_manager.db import create_db
import metadata_manager.tests.utils as utils


@pytest.mark.c0c0658d55
@pytest.mark.manager
@pytest.mark.sanity
def test__create_git_repository(working_dir):
    assert utils.initalize_git_repository(working_dir)
    assert (working_dir / ".git").exists()


@pytest.mark.c5dbb518a4
@pytest.mark.manager
@pytest.mark.sanity
def test_create_md_dirs(working_dir, metadata_dir):
    create_md_dirs(working_dir)
    assert (working_dir / metadata_dir).exists()
    assert (working_dir / metadata_dir / "hashes").exists()
    assert (working_dir / metadata_dir / "deleted").exists()


@pytest.mark.ab14fccc2b
@pytest.mark.manager
@pytest.mark.sanity
def test_dir_is_managed_by_md(working_dir):
    # Test the path to the root, don't care about the result here
    # Just test that the function executes successfully.
    check_dir_is_md_managed(working_dir)

    subdir1 = Path("dir1")
    subdir2 = Path("dir2")
    os.makedirs(working_dir / subdir1 / subdir2)

    assert not check_dir_is_md_managed(
        working_dir / subdir1 / subdir2, stop_at=working_dir
    )

    create_md_dirs(working_dir)
    assert check_dir_is_md_managed(working_dir / subdir1 / subdir2)
    assert check_dir_is_md_managed(working_dir / subdir1)
    assert check_dir_is_md_managed(working_dir)


@pytest.mark.b2d798a933
@pytest.mark.manager
@pytest.mark.sanity
def test_dir_is_managed_by_md_stop_at(working_dir):
    subdir1 = Path("dir1")
    subdir2 = Path("dir2")
    os.makedirs(working_dir / subdir1 / subdir2)

    create_md_dirs(working_dir)

    assert not check_dir_is_md_managed(
        working_dir / subdir1 / subdir2, stop_at=working_dir / subdir1 / subdir2
    )
    assert not check_dir_is_md_managed(
        working_dir / subdir1 / subdir2, stop_at=working_dir / subdir1
    )
    assert check_dir_is_md_managed(working_dir / subdir1 / subdir2, stop_at=working_dir)


@pytest.mark.fe98f4c7f9
@pytest.mark.manager
@pytest.mark.sanity
def test_dir_is_managed_by_git(working_dir):
    # Test the path to the root, don't care about the result here
    # Just test that the function executes successfully.
    check_dir_is_git_managed(working_dir)

    subdir1 = Path("dir1")
    subdir2 = Path("dir2")
    os.makedirs(working_dir / subdir1 / subdir2)

    assert not check_dir_is_git_managed(
        working_dir / subdir1 / subdir2, stop_at=working_dir
    )

    assert utils.initalize_git_repository(working_dir)
    assert (working_dir / ".git").exists()

    assert check_dir_is_git_managed(working_dir / subdir1 / subdir2)
    assert check_dir_is_git_managed(working_dir / subdir1)
    assert check_dir_is_git_managed(working_dir)


@pytest.mark.f670bc15f0
@pytest.mark.manager
@pytest.mark.sanity
def test_dir_is_managed_by_git_stop_at(working_dir):
    subdir1 = Path("dir1")
    subdir2 = Path("dir2")
    os.makedirs(working_dir / subdir1 / subdir2)

    assert utils.initalize_git_repository(working_dir)

    print(os.listdir(working_dir))

    assert not check_dir_is_git_managed(
        working_dir / subdir1 / subdir2, stop_at=working_dir / subdir1 / subdir2
    )
    assert not check_dir_is_git_managed(
        working_dir / subdir1 / subdir2, stop_at=working_dir / subdir1
    )
    assert check_dir_is_git_managed(
        working_dir / subdir1 / subdir2, stop_at=working_dir
    )


@pytest.mark.f9308dd389
@pytest.mark.manager
@pytest.mark.sanity
@pytest.mark.cleanup
def test_cleanup(working_dir, metadata_dir):
    create_md_dirs(working_dir)
    create_db(working_dir / metadata_dir)
    utils.assert_md_structure_exists(working_dir)
    cleanup(working_dir)
    assert not (working_dir / metadata_dir).exists()
