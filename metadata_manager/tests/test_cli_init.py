import subprocess
import pytest

import metadata_manager.tests.utils as utils
from metadata_manager.messages import Messages


@pytest.mark.ebff0e4472
@pytest.mark.cli
@pytest.mark.init_subcommand
@pytest.mark.sanity
def test_init_creates_md_repository_in_cwd(
    working_dir, init_cmd, monkeypatch, md_manager
):
    monkeypatch.chdir(working_dir)
    proc = subprocess.run([*init_cmd], capture_output=True)
    assert not proc.stderr
    assert proc.returncode == 0
    utils.assert_md_structure_exists(md_manager.md_config, working_dir)
    utils.assert_database_structure(
        working_dir / md_manager.md_config.md_dir_name / md_manager.md_config.md_db_name
    )
    assert Messages.init_success_messages.text in str(proc.stdout)


@pytest.mark.ed68aa1433
@pytest.mark.cli
@pytest.mark.init_subcommand
@pytest.mark.sanity
def test_init_creates_md_repository_in_target_dir(working_dir, init_cmd, md_manager):
    subdir = working_dir / "dir1"
    subdir.mkdir()
    proc = subprocess.run([*init_cmd, subdir], capture_output=True)
    assert proc.returncode == 0
    utils.assert_md_structure_exists(md_manager.md_config, working_dir / "dir1")
    utils.assert_database_structure(
        working_dir
        / "dir1"
        / md_manager.md_config.md_dir_name
        / md_manager.md_config.md_db_name
    )
    assert Messages.init_success_messages.text in str(proc.stdout)


@pytest.mark.d4b42a1511
@pytest.mark.cli
@pytest.mark.init_subcommand
@pytest.mark.sanity
def test_init_creates_both_md_respository_and_target_dir_if_it_doesnt_exist(
    working_dir, init_cmd, md_manager
):
    subdir = working_dir / "dir1" / "dir2"

    proc = subprocess.run([*init_cmd, subdir], capture_output=True)
    assert proc.returncode == 0
    assert not proc.stderr

    assert subdir.exists()
    utils.assert_md_structure_exists(md_manager.md_config, subdir)
    utils.assert_database_structure(
        subdir / md_manager.md_config.md_dir_name / md_manager.md_config.md_db_name
    )


@pytest.mark.b767b0b432
@pytest.mark.cli
@pytest.mark.init_subcommand
@pytest.mark.sanity
def test_init_aborts_when_another_md_is_detected_in_the_same_dir(
    working_dir, init_cmd, md_manager
):
    subdir = working_dir / "dir1"
    subdir.mkdir()
    proc = subprocess.run([*init_cmd, subdir], capture_output=True)
    assert proc.returncode == 0
    utils.assert_md_structure_exists(md_manager.md_config, subdir)

    proc = subprocess.run([*init_cmd, subdir], capture_output=True)
    assert proc.returncode == 1
    assert "Abort" in str(proc.stderr)

    # These should still exist from the previous run.
    # Make sure they are not cleaned up.
    utils.assert_md_structure_exists(md_manager.md_config, subdir)
    utils.assert_database_structure(
        subdir / md_manager.md_config.md_dir_name / md_manager.md_config.md_db_name
    )


@pytest.mark.ad4d569613
@pytest.mark.cli
@pytest.mark.init_subcommand
@pytest.mark.sanity
def test_init_aborts_when_another_md_is_detected_on_the_path_to_root(
    working_dir, init_cmd, md_manager
):
    subdir = working_dir / "dir1" / "dir2"
    subdir.mkdir(parents=True)
    proc = subprocess.run([*init_cmd, working_dir], capture_output=True)
    assert proc.returncode == 0
    utils.assert_md_structure_exists(md_manager.md_config, working_dir)

    proc = subprocess.run([*init_cmd, subdir], capture_output=True)
    assert proc.returncode == 1
    assert "abort" in str(proc.stderr).lower()

    # These should still exist from the previous run.
    # Make sure they are not cleaned up.
    utils.assert_md_structure_exists(md_manager.md_config, working_dir)
    utils.assert_database_structure(
        working_dir / md_manager.md_config.md_dir_name / md_manager.md_config.md_db_name
    )


@pytest.mark.c68c2b29d1
@pytest.mark.cli
@pytest.mark.init_subcommand
@pytest.mark.sanity
def test_init_aborts_when_git_is_detected_in_the_same_dir(working_dir, init_cmd):
    assert utils.initalize_git_repository(working_dir)

    proc = subprocess.run([*init_cmd, working_dir], capture_output=True)
    assert proc.returncode == 1
    assert "abort" in str(proc.stderr).lower()


@pytest.mark.be560f3207
@pytest.mark.cli
@pytest.mark.init_subcommand
@pytest.mark.sanity
def test_init_aborts_when_git_is_detected_on_the_path_to_root(working_dir, init_cmd):
    subdir = working_dir / "dir1" / "dir2"
    subdir.mkdir(parents=True)

    assert utils.initalize_git_repository(working_dir)

    proc = subprocess.run([*init_cmd, subdir], capture_output=True)
    assert proc.returncode == 1
    assert "abort" in str(proc.stderr).lower()


@pytest.mark.be560f3207
@pytest.mark.cli
@pytest.mark.init_subcommand
@pytest.mark.sanity
@pytest.mark.parametrize("yes_flag", ["-y", "--yes"])
def test_init_bypases_git_check_if_yes_flag_is_provided(
    working_dir, init_cmd, yes_flag
):
    assert utils.initalize_git_repository(working_dir)

    proc = subprocess.run([*init_cmd, working_dir, yes_flag], capture_output=True)
    assert proc.returncode == 0
    assert not proc.stderr
    assert Messages.init_success_messages.text in str(proc.stdout)
