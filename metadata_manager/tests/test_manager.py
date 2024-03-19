import pytest
from pathlib import Path
import os

from metadata_manager.db import create_db
import metadata_manager.tests.utils as utils


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
    create_db(
        working_dir / md_manager.md_config.md_dir_name, md_manager.md_config.md_db_name
    )
    utils.assert_md_structure_exists(md_manager.md_config, working_dir)
    md_manager.cleanup(working_dir)
    assert not (working_dir / md_manager.md_config.md_dir_name).exists()
