import subprocess
import pytest
from pathlib import Path
import shutil

from sqlalchemy.orm import Session

import tests.utils as utils
from build import write_build_info
from md_enums import BuildType
from md_models import VersionInfoORM
from db import create_or_get_session


@pytest.mark.ebff0e4472
@pytest.mark.cli
@pytest.mark.init
@pytest.mark.sanity
def test_init_creates_md_repository_in_cwd(working_dir, mdm):
    utils.assert_md_structure_exists(mdm.md_config, working_dir)
    utils.assert_database_structure(mdm.md_db_path)


@pytest.mark.ed68aa1433
@pytest.mark.cli
@pytest.mark.init
@pytest.mark.sanity
def test_init_creates_md_repository_in_target_dir(working_dir, init_cmd, mdm):
    subdir = working_dir.joinpath("dir1")
    subdir.mkdir()
    shutil.rmtree(mdm.md_path)

    subprocess.check_output([*init_cmd, subdir])
    utils.assert_md_structure_exists(mdm.md_config, working_dir / "dir1")
    utils.assert_database_structure(
        working_dir / "dir1" / mdm.md_config.md_dir_name / mdm.md_config.md_db_name
    )


@pytest.mark.cae7feba85
@pytest.mark.cli
@pytest.mark.init
@pytest.mark.sanity
@pytest.mark.parametrize("target_path", [".", "dir1", "./dir1", "dir1/dir2"])
def test_init_creates_md_repository_in_target_dir_with_relative_path(
    init_cmd, mdm, target_path, working_dir
):
    subdir = working_dir.joinpath(target_path)
    subdir.mkdir(parents=True, exist_ok=True)
    shutil.rmtree(mdm.md_path)

    subprocess.check_output([*init_cmd, subdir])
    utils.assert_md_structure_exists(mdm.md_config, Path(target_path))
    utils.assert_database_structure(
        db_path=subdir.joinpath(mdm.md_config.md_dir_name, mdm.md_config.md_db_name)
    )


@pytest.mark.d4b42a1511
@pytest.mark.cli
@pytest.mark.init
@pytest.mark.sanity
def test_init_creates_both_md_respository_and_target_dir_if_it_doesnt_exist(
    working_dir, init_cmd, mdm
):
    shutil.rmtree(mdm.md_path)
    subdir = working_dir.joinpath("dir1", "dir2")

    subprocess.check_output([*init_cmd, subdir])

    assert subdir.exists()
    utils.assert_md_structure_exists(mdm.md_config, subdir)
    utils.assert_database_structure(
        db_path=subdir.joinpath(mdm.md_config.md_dir_name, mdm.md_config.md_db_name)
    )


@pytest.mark.b767b0b432
@pytest.mark.cli
@pytest.mark.init
@pytest.mark.sanity
def test_init_aborts_when_another_md_is_detected_in_the_same_dir(
    working_dir, init_cmd, mdm
):
    # NOTE: mdm fixture automatically created mdm repository. So at this
    # point, mdm repository already exist int the working directory.
    proc = subprocess.run([*init_cmd, working_dir], capture_output=True)
    assert proc.returncode == 200
    assert "Abort" in str(proc.stderr)

    # These should still exist.
    # Make sure they are not cleaned up.
    utils.assert_md_structure_exists(md_config=mdm.md_config, where=working_dir)
    utils.assert_database_structure(db_path=mdm.md_db_path)


@pytest.mark.be334fb0aa
@pytest.mark.cli
@pytest.mark.init
@pytest.mark.sanity
@pytest.mark.preserve_version_data
def test_init_creates_version_info_record(
    mdm_config, monkeypatch, working_dir, init_cmd
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

    session_or_err = create_or_get_session(
        db_path=working_dir.joinpath(mdm_config.md_dir_name, mdm_config.md_db_name)
    )
    assert isinstance(session_or_err, Session)

    version_info = session_or_err.query(VersionInfoORM).first()

    assert version_info.version == expected_version
    assert version_info.commit_id == expected_commit_id
    assert version_info.build_type.value == expected_build_type.value
