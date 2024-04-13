import subprocess
import pytest
from pathlib import Path
import shutil

import tests.utils as utils
from build import write_build_info
from md_enums import BuildType
from models.local_models import VersionInfoORM, RepositoryORM
from models.global_models import RepositoriesORM
from db import get_local_session_or_exit
from manager import MetadataManager


@pytest.mark.ebff0e4472
@pytest.mark.cli
@pytest.mark.init
@pytest.mark.sanity
def test_init_creates_md_repository_in_cwd(working_dir, mdm):
    utils.assert_md_structure_exists(mdm.md_config, working_dir)
    utils.assert_database_structure(mdm.db_path)


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
        working_dir
        / "dir1"
        / mdm.md_config.local_dir_name
        / mdm.md_config.local_db_name
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
        db_path=subdir.joinpath(
            mdm.md_config.local_dir_name, mdm.md_config.local_db_name
        )
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
        db_path=subdir.joinpath(
            mdm.md_config.local_dir_name, mdm.md_config.local_db_name
        )
    )


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

    session = get_local_session_or_exit(
        db_path=working_dir.joinpath(
            mdm_config.local_dir_name, mdm_config.local_db_name
        )
    )

    version_info = session.query(VersionInfoORM).first()
    assert version_info.version == expected_version
    assert version_info.commit_id == expected_commit_id
    assert version_info.build_type.value == expected_build_type.value

    session.close()


@pytest.mark.f65ee82ab6
@pytest.mark.cli
@pytest.mark.init
@pytest.mark.sanity
def test_init_creates_repository_table_without_parent_repo_info(
    mdm_config, working_dir, init_cmd
):
    # Validate repository table was created duing initialization of
    # a new repository. Parent repository's info should be set to null if
    # parent repository doesn't exist.

    subprocess.check_output([*init_cmd, working_dir])
    mdm = MetadataManager.from_repository(path=working_dir, md_config=mdm_config)
    session = get_local_session_or_exit(db_path=mdm.db_path)

    repository_record = session.query(RepositoryORM).first()
    assert str(repository_record.repository_filepath) == str(working_dir)
    assert not repository_record.parent_repository_id
    assert not repository_record.parent_repository_filepath

    session.close()


@pytest.mark.d1d2d4cee1
@pytest.mark.cli
@pytest.mark.init
@pytest.mark.sanity
def test_init_creates_repository_table_with_parent_repo_info(
    mdm_config, working_dir, init_cmd, monkeypatch
):
    # Validate repository table was created duing initialization of
    # a new repository. Parent repository's info should be correctly
    # filled in if parent repository exists.

    subprocess.check_output([*init_cmd, working_dir])
    mdm = MetadataManager.from_repository(path=working_dir, md_config=mdm_config)
    session = get_local_session_or_exit(db_path=mdm.db_path)

    parent_repository_record = session.query(RepositoryORM).first()
    assert parent_repository_record

    child_repo_dir = working_dir.joinpath("child_dir")
    child_repo_dir.mkdir()
    monkeypatch.chdir(child_repo_dir)
    proc = subprocess.run(
        [*init_cmd, child_repo_dir, "--load-from-parent-repository", "--debug"],
        capture_output=True,
    )
    assert proc.returncode == 0

    child_mdm = MetadataManager.from_repository(
        path=child_repo_dir, md_config=mdm_config
    )
    child_session = get_local_session_or_exit(db_path=child_mdm.db_path)

    child_repository_record = child_session.query(RepositoryORM).first()
    assert str(child_repository_record.repository_filepath) == str(child_repo_dir)
    assert str(child_repository_record.parent_repository_filepath) == str(working_dir)
    assert child_repository_record.parent_repository_id == parent_repository_record.id

    session.close()
    child_session.close()


@pytest.mark.f2132ac22e
@pytest.mark.cli
@pytest.mark.init
@pytest.mark.sanity
def test_init_creates_record_in_global_database(
    init_cmd, working_dir, global_session, mdm_config
):
    subprocess.check_output([*init_cmd, working_dir])

    local_session = get_local_session_or_exit(
        db_path=working_dir.joinpath(
            mdm_config.local_dir_name, mdm_config.local_db_name
        )
    )

    local_repository_record = local_session.query(RepositoryORM).first()
    assert local_repository_record

    assert (
        global_session.query(RepositoriesORM)
        .filter_by(id=local_repository_record.id)
        .first()
    )


@pytest.mark.f406dc4dd9
@pytest.mark.cli
@pytest.mark.init
@pytest.mark.sanity
def test_init_updates_repository_id_if_respoitory_exists_at_given_path_with_recreate(
    init_cmd, working_dir, global_session, mdm_config
):
    subprocess.check_output([*init_cmd, working_dir])

    local_session = get_local_session_or_exit(
        db_path=working_dir.joinpath(
            mdm_config.local_dir_name, mdm_config.local_db_name
        )
    )

    local_repository_record_1 = local_session.query(RepositoryORM).first()
    local_session.close()
    assert local_repository_record_1

    assert (
        global_session.query(RepositoriesORM)
        .filter_by(id=local_repository_record_1.id)
        .first()
    )

    subprocess.check_output([*init_cmd, working_dir, "--recreate"])

    local_session = get_local_session_or_exit(
        db_path=working_dir.joinpath(
            mdm_config.local_dir_name, mdm_config.local_db_name
        )
    )

    local_repository_record_2 = local_session.query(RepositoryORM).first()
    local_session.close()
    assert local_repository_record_2

    assert (
        not global_session.query(RepositoriesORM)
        .filter_by(id=local_repository_record_1.id)
        .first()
    )
    assert (
        global_session.query(RepositoriesORM)
        .filter_by(id=local_repository_record_2.id)
        .first()
    )


@pytest.mark.ab51d1defa
@pytest.mark.cli
@pytest.mark.init
@pytest.mark.sanity
def test_init_updates_repository_id_if_respoitory_exists_at_given_path_without_recreate(
    init_cmd, working_dir, global_session, mdm_config
):
    subprocess.check_output([*init_cmd, working_dir])

    local_session = get_local_session_or_exit(
        db_path=working_dir.joinpath(
            mdm_config.local_dir_name, mdm_config.local_db_name
        )
    )

    local_repository_record_1 = local_session.query(RepositoryORM).first()
    local_session.close()
    assert local_repository_record_1

    assert (
        global_session.query(RepositoriesORM)
        .filter_by(id=local_repository_record_1.id)
        .first()
    )

    shutil.rmtree(working_dir.joinpath(mdm_config.local_dir_name))
    subprocess.check_output([*init_cmd, working_dir])

    local_session = get_local_session_or_exit(
        db_path=working_dir.joinpath(
            mdm_config.local_dir_name, mdm_config.local_db_name
        )
    )

    local_repository_record_2 = local_session.query(RepositoryORM).first()
    local_session.close()
    assert local_repository_record_2

    assert (
        not global_session.query(RepositoriesORM)
        .filter_by(id=local_repository_record_1.id)
        .first()
    )
    assert (
        global_session.query(RepositoriesORM)
        .filter_by(id=local_repository_record_2.id)
        .first()
    )
