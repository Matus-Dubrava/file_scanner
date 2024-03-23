import subprocess
import pytest
from pathlib import Path

import tests.utils as utils
from build import write_build_info
from md_enums import BuildType
from md_models import VersionInfoORM
from db import get_session


@pytest.mark.ebff0e4472
@pytest.mark.cli
@pytest.mark.init
@pytest.mark.sanity
def test_init_creates_md_repository_in_cwd(working_dir, init_cmd, md_manager):
    subprocess.check_output([*init_cmd])
    utils.assert_md_structure_exists(md_manager.md_config, working_dir)
    utils.assert_database_structure(
        working_dir / md_manager.md_config.md_dir_name / md_manager.md_config.md_db_name
    )


@pytest.mark.ed68aa1433
@pytest.mark.cli
@pytest.mark.init
@pytest.mark.sanity
def test_init_creates_md_repository_in_target_dir(working_dir, init_cmd, md_manager):
    subdir = working_dir / "dir1"
    subdir.mkdir()
    subprocess.check_output([*init_cmd, subdir])
    utils.assert_md_structure_exists(md_manager.md_config, working_dir / "dir1")
    utils.assert_database_structure(
        working_dir
        / "dir1"
        / md_manager.md_config.md_dir_name
        / md_manager.md_config.md_db_name
    )


@pytest.mark.cae7feba85
@pytest.mark.cli
@pytest.mark.init
@pytest.mark.sanity
@pytest.mark.parametrize("target_path", [".", "dir1", "./dir1", "dir1/dir2"])
def test_init_creates_md_repository_in_target_dir_with_relative_path(
    init_cmd, md_manager, target_path, working_dir
):
    subprocess.check_output([*init_cmd, target_path])
    utils.assert_md_structure_exists(md_manager.md_config, Path(target_path))
    utils.assert_database_structure(
        Path(target_path)
        / md_manager.md_config.md_dir_name
        / md_manager.md_config.md_db_name
    )


@pytest.mark.d4b42a1511
@pytest.mark.cli
@pytest.mark.init
@pytest.mark.sanity
def test_init_creates_both_md_respository_and_target_dir_if_it_doesnt_exist(
    working_dir, init_cmd, md_manager
):
    subdir = working_dir / "dir1" / "dir2"

    subprocess.check_output([*init_cmd, subdir])

    assert subdir.exists()
    utils.assert_md_structure_exists(md_manager.md_config, subdir)
    utils.assert_database_structure(
        subdir / md_manager.md_config.md_dir_name / md_manager.md_config.md_db_name
    )


@pytest.mark.b767b0b432
@pytest.mark.cli
@pytest.mark.init
@pytest.mark.sanity
def test_init_aborts_when_another_md_is_detected_in_the_same_dir(
    working_dir, init_cmd, md_manager
):
    subdir = working_dir / "dir1"
    subdir.mkdir()
    subprocess.check_output([*init_cmd, subdir])
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
@pytest.mark.init
@pytest.mark.sanity
def test_init_aborts_when_another_md_is_detected_on_the_path_to_root(
    working_dir, init_cmd, md_manager
):
    subdir = working_dir / "dir1" / "dir2"
    subdir.mkdir(parents=True)
    subprocess.check_output([*init_cmd, working_dir])
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


@pytest.mark.be334fb0aa
@pytest.mark.cli
@pytest.mark.init
@pytest.mark.sanity
@pytest.mark.preserve_version_data
def test_init_creates_version_info_record(
    working_dir,
    init_cmd,
    md_manager,
    monkeypatch,
):
    monkeypatch.chdir(Path(__file__).parent)
    expected_commit_id = (
        subprocess.check_output(["git", "rev-parse", "HEAD"]).decode().strip()
    )
    expected_version = "2.0.0"
    expected_build_type = BuildType.DEV

    maybe_err = write_build_info(
        version=expected_version, build_type=expected_build_type
    )
    if maybe_err:
        raise maybe_err

    subprocess.check_output([*init_cmd, working_dir])

    db_dir = working_dir / md_manager.md_config.md_dir_name
    session = get_session(db_dir=db_dir, db_name=md_manager.md_config.md_db_name)

    version_info = session.query(VersionInfoORM).first()

    assert version_info.commit_id == expected_commit_id
    assert version_info.build_type.value == expected_build_type.value
    assert version_info.version == expected_version
