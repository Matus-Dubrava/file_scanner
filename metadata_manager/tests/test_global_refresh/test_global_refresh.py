import pytest
import shutil

from manager import MetadataManager
from db import LocalSessionOrExit, GlobalSessionOrExit
import tests.utils as test_utils


@pytest.mark.ca057717a5
@pytest.mark.global_
@pytest.mark.refresh
@pytest.mark.sanity
def test_global_manager_produces_refresh_summary(
    mdm_config, working_dir, global_manager
):
    with GlobalSessionOrExit(db_path=global_manager.db_path) as global_session:
        outcome = global_manager.refresh_all_repositories(
            session=global_session, debug=True
        )

        # No records
        assert outcome.total_repositories == 0
        assert outcome.refreshed_repositories == 0
        assert outcome.failed_repositories == 0
        assert outcome.total_files == 0
        assert outcome.refreshed_files == 0
        assert outcome.refreshed_repositories_with_errors == 0
        assert outcome.failed_files == 0

    # Create empty repos.
    dir_1 = working_dir.joinpath("dir_1")
    dir_1.mkdir()
    mdm1 = MetadataManager.new(md_config=mdm_config, path=working_dir)
    mdm2 = MetadataManager.new(md_config=mdm_config, path=dir_1)

    with GlobalSessionOrExit(db_path=global_manager.db_path) as global_session:
        outcome = global_manager.refresh_all_repositories(
            session=global_session, debug=True
        )

        assert outcome.total_repositories == 2
        assert outcome.refreshed_repositories == 2
        assert outcome.failed_repositories == 0
        assert outcome.total_files == 0
        assert outcome.refreshed_files == 0
        assert outcome.refreshed_repositories_with_errors == 0
        assert outcome.failed_files == 0

    # Create some files.
    file_1 = working_dir.joinpath("file_1")
    file_2 = working_dir.joinpath("file_2")
    file_3 = dir_1.joinpath("file_3")

    with LocalSessionOrExit(db_path=mdm1.db_path) as local_session:
        mdm1.touch(session=local_session, filepath=file_1)
        mdm1.touch(session=local_session, filepath=file_2)

    with LocalSessionOrExit(db_path=mdm2.db_path) as local_session:
        mdm2.touch(session=local_session, filepath=file_3)

    with GlobalSessionOrExit(db_path=global_manager.db_path) as global_session:
        outcome = global_manager.refresh_all_repositories(
            session=global_session, debug=True
        )

        assert outcome.total_repositories == 2
        assert outcome.refreshed_repositories == 2
        assert outcome.failed_repositories == 0
        assert outcome.total_files == 3
        assert outcome.refreshed_files == 3
        assert outcome.refreshed_repositories_with_errors == 0
        assert outcome.failed_files == 0

    # Remove file this should result in one error.
    file_1.unlink()

    with GlobalSessionOrExit(db_path=global_manager.db_path) as global_session:
        outcome = global_manager.refresh_all_repositories(
            session=global_session, debug=True
        )

        assert outcome.total_repositories == 2
        assert outcome.refreshed_repositories == 1
        assert outcome.refreshed_repositories_with_errors == 1
        assert outcome.failed_repositories == 0
        assert outcome.total_files == 3
        assert outcome.refreshed_files == 2
        assert outcome.failed_files == 1

    # Remove file via manager. This shouldn't result in error.
    with LocalSessionOrExit(db_path=mdm2.db_path) as local_session:
        mdm2.remove_file(session=local_session, filepath=file_3)

    with GlobalSessionOrExit(db_path=global_manager.db_path) as global_session:
        outcome = global_manager.refresh_all_repositories(
            session=global_session, debug=True
        )

        assert outcome.total_repositories == 2
        assert outcome.refreshed_repositories == 1
        assert outcome.refreshed_repositories_with_errors == 1
        assert outcome.failed_repositories == 0
        assert outcome.total_files == 2
        assert outcome.refreshed_files == 1
        assert outcome.failed_files == 1

    # Delete repository. This one should be skipped.
    shutil.rmtree(dir_1)

    with GlobalSessionOrExit(db_path=global_manager.db_path) as global_session:
        outcome = global_manager.refresh_all_repositories(
            session=global_session, debug=True
        )

        assert outcome.total_repositories == 1
        assert outcome.refreshed_repositories == 0
        assert outcome.refreshed_repositories_with_errors == 1
        assert outcome.failed_repositories == 0
        assert outcome.total_files == 2
        assert outcome.refreshed_files == 1
        assert outcome.failed_files == 1


@pytest.mark.f707922b95
@pytest.mark.global_
@pytest.mark.refresh
@pytest.mark.sanity
def test_global_manager_produces_refresh_statistics_with_failed_repositories(
    mdm_config, working_dir, global_manager
):

    mdm = MetadataManager.new(md_config=mdm_config, path=working_dir)

    with LocalSessionOrExit(db_path=mdm.db_path) as local_session:
        mdm.touch(session=local_session, filepath=working_dir.joinpath("file_"))

    test_utils.corrupt_sqlite_file(mdm.db_path)

    with GlobalSessionOrExit(db_path=global_manager.db_path) as global_session:
        outcome = global_manager.refresh_all_repositories(
            session=global_session, debug=True
        )

        assert outcome.total_repositories == 1
        assert outcome.refreshed_repositories == 0
        assert outcome.refreshed_repositories_with_errors == 0
        assert outcome.failed_repositories == 1
        assert outcome.total_files == 0
        assert outcome.refreshed_files == 0
        assert outcome.failed_files == 0
