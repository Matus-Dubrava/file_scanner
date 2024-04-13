import pytest
import shutil
import subprocess

from manager import MetadataManager
from db import LocalSessionOrExit
from models.local_models import HistoryORM


@pytest.mark.bbdc79ade0
@pytest.mark.cli
@pytest.mark.global_
@pytest.mark.refresh
@pytest.mark.sanity
def test_global_refresh_refreshes_all_repositories(
    working_dir, mdm_config, global_refresh_cmd
):
    dir_ = working_dir.joinpath("dir_")
    dir_.mkdir()

    mdm1 = MetadataManager.new(path=working_dir, md_config=mdm_config)
    mdm2 = MetadataManager.new(path=dir_, md_config=mdm_config)

    file_1 = working_dir.joinpath("file_1")
    file_2 = dir_.joinpath("file_2")

    with LocalSessionOrExit(db_path=mdm1.db_path) as local_session:
        mdm1.touch(session=local_session, filepath=file_1)

    with LocalSessionOrExit(db_path=mdm2.db_path) as local_session:
        mdm2.touch(session=local_session, filepath=file_2)

    subprocess.check_output([*global_refresh_cmd])

    with LocalSessionOrExit(db_path=mdm1.db_path) as local_session:
        assert local_session.query(HistoryORM).filter_by(filepath=file_1).count() == 2

    with LocalSessionOrExit(db_path=mdm2.db_path) as local_session:
        assert local_session.query(HistoryORM).filter_by(filepath=file_2).count() == 2


@pytest.mark.b2084fda22
@pytest.mark.cli
@pytest.mark.global_
@pytest.mark.refresh
@pytest.mark.sanity
def test_global_refresh_skips_missing_repositories(
    working_dir, mdm_config, global_refresh_cmd
):
    """
    Expecting that 'refresh' works even when there are repository records
    for which actual repositories were removed.
    """

    dir_ = working_dir.joinpath("dir_1")
    dir_.mkdir()

    mdm1 = MetadataManager.new(path=working_dir, md_config=mdm_config)
    mdm2 = MetadataManager.new(path=dir_, md_config=mdm_config)

    file_1 = working_dir.joinpath("file_1")
    file_2 = dir_.joinpath("file_2")

    with LocalSessionOrExit(db_path=mdm1.db_path) as local_session:
        mdm1.touch(session=local_session, filepath=file_1)

    with LocalSessionOrExit(db_path=mdm2.db_path) as local_session:
        mdm2.touch(session=local_session, filepath=file_2)

    # Remove internal repository dir.
    shutil.rmtree(working_dir.joinpath(mdm_config.local_dir_name))
    subprocess.check_output([*global_refresh_cmd])

    with LocalSessionOrExit(db_path=mdm2.db_path) as local_session:
        assert local_session.query(HistoryORM).filter_by(filepath=file_2).count() == 2


@pytest.mark.f6310a6e51
@pytest.mark.cli
@pytest.mark.global_
@pytest.mark.refresh
@pytest.mark.sanity
def test_global_refresh_works_with_no_respoitory_records(
    global_refresh_cmd, mdm_config, working_dir
):
    """
    Test 'refresh' doesn't fail when there are no valid repository records.
    """

    MetadataManager.new(md_config=mdm_config, path=working_dir)
    shutil.rmtree(working_dir.joinpath(mdm_config.local_dir_name))

    subprocess.check_output([*global_refresh_cmd])
