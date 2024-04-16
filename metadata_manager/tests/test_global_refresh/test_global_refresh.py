import pytest
import shutil

from manager import MetadataManager
from db import LocalSessionOrExit, GlobalSessionOrExit
import tests.utils as test_utils
from models.global_models import RefreshLogORM, RefreshFileORM, RefreshRepositoryORM


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


@pytest.mark.c0ebac95ce
@pytest.mark.global_
@pytest.mark.refresh
@pytest.mark.sanity
def test_global_manager_write_refresh_statisitcs_to_database(
    mdm_config, working_dir, global_manager
):
    dir_1 = working_dir.joinpath("dir_")
    dir_1.mkdir()
    mdm1 = MetadataManager.new(md_config=mdm_config, path=working_dir)
    mdm2 = MetadataManager.new(md_config=mdm_config, path=dir_1)

    file_1 = working_dir.joinpath("file_1")
    file_2 = dir_1.joinpath("file_2")
    file_1.write_text("test\ntest")

    with LocalSessionOrExit(db_path=mdm1.db_path) as local_session:
        mdm1.touch(session=local_session, filepath=file_1)

    with LocalSessionOrExit(db_path=mdm2.db_path) as local_session:
        mdm2.touch(session=local_session, filepath=file_2)

    with GlobalSessionOrExit(db_path=global_manager.db_path) as global_session:
        # REFRESH 1:
        global_manager.refresh_all_repositories(session=global_session)

        assert global_session.query(RefreshLogORM).count() == 1
        assert (
            global_session.query(RefreshRepositoryORM)
            .filter_by(path=working_dir)
            .count()
            == 1
        )
        assert (
            global_session.query(RefreshRepositoryORM).filter_by(path=dir_1).count()
            == 1
        )

        # refresh 1 repo 1
        test_utils.assert_latest_refresh_repository_record(
            session=global_session,
            path=mdm1.repository_root,
            error_occured=False,
            total_files=1,
            files_refreshed=1,
            files_failed=0,
        )

        # refresh 1 repo 2
        test_utils.assert_latest_refresh_repository_record(
            session=global_session,
            path=mdm2.repository_root,
            error_occured=False,
            total_files=1,
            files_refreshed=1,
            files_failed=0,
        )

        # refresh 1 file 1
        test_utils.assert_latest_refresh_file_record(
            session=global_session,
            repository_path=mdm1.repository_root,
            filepath=file_1,
            error_occured=False,
            lines_added=0,
            lines_removed=0,
            running_lines_added=2,
            running_lines_removed=0,
        )

        # refresh 1 file 2
        assert global_session.query(RefreshFileORM).filter_by(path=file_2).count() == 1

        test_utils.assert_latest_refresh_file_record(
            session=global_session,
            repository_path=mdm2.repository_root,
            filepath=file_2,
            error_occured=False,
            lines_added=0,
            lines_removed=0,
            running_lines_added=0,
            running_lines_removed=0,
        )

        # REFRESH 2:
        # - simulating repository 2 refresh fail
        # - adding new file to repository 1
        # - updating lines in file 1
        test_utils.corrupt_sqlite_file(path=mdm2.db_path)

        with LocalSessionOrExit(db_path=mdm1.db_path) as local_session:
            file_3 = working_dir.joinpath("file_3")
            mdm1.touch(session=local_session, filepath=file_3)

        file_3.write_text("test\ntest\ntest")
        file_1.write_text("newline")

        global_manager.refresh_all_repositories(session=global_session)

        assert global_session.query(RefreshLogORM).count() == 2
        assert (
            global_session.query(RefreshRepositoryORM)
            .filter_by(path=working_dir)
            .count()
            == 2
        )
        assert (
            global_session.query(RefreshRepositoryORM).filter_by(path=dir_1).count()
            == 2
        )

        # since refresh for repo 2 failed, file counts should be set to None
        test_utils.assert_latest_refresh_repository_record(
            session=global_session, path=mdm2.repository_root, error_occured=True
        )

        # there should be no file refresh records for repo 2 second refresh
        test_utils.assert_count_latest_file_records(
            session=global_session, repository_path=mdm2.repository_root, count=0
        )

        # repo 1 should have 2 files refreshed
        test_utils.assert_latest_refresh_repository_record(
            session=global_session,
            path=mdm1.repository_root,
            error_occured=False,
            total_files=2,
            files_refreshed=2,
            files_failed=0,
        )

        # refresh 2 file 1
        test_utils.assert_latest_refresh_file_record(
            session=global_session,
            repository_path=mdm1.repository_root,
            filepath=file_1,
            error_occured=False,
            lines_added=1,
            lines_removed=2,
            running_lines_added=3,
            running_lines_removed=2,
        )

        # refresh 2 file 2 should not have been recorded since refresh for repository 2 failed
        test_utils.assert_count_latest_file_records(
            session=global_session, repository_path=mdm2.repository_root, count=0
        )

        # refresh 2 file 3
        test_utils.assert_latest_refresh_file_record(
            session=global_session,
            repository_path=mdm1.repository_root,
            filepath=file_3,
            error_occured=False,
            lines_added=3,
            lines_removed=0,
            running_lines_added=3,
            running_lines_removed=0,
        )

        # REFRESH 3
        # - simulating manual removal of file 3
        file_3.unlink()
        global_manager.refresh_all_repositories(session=global_session)

        assert global_session.query(RefreshLogORM).count() == 3
        assert (
            global_session.query(RefreshRepositoryORM)
            .filter_by(path=working_dir)
            .count()
            == 3
        )
        assert (
            global_session.query(RefreshRepositoryORM).filter_by(path=dir_1).count()
            == 3
        )

        # refresh 3 repo 1
        test_utils.assert_latest_refresh_repository_record(
            session=global_session,
            path=mdm1.repository_root,
            error_occured=False,
            files_failed=1,
            total_files=2,
            files_refreshed=1,
        )

        test_utils.assert_count_latest_file_records(
            session=global_session, count=2, repository_path=mdm1.repository_root
        )

        # refresh 3 repo 2
        test_utils.assert_latest_refresh_repository_record(
            session=global_session, path=mdm2.repository_root, error_occured=True
        )

        test_utils.assert_count_latest_file_records(
            session=global_session, count=0, repository_path=mdm2.repository_root
        )

        # refresh 3 file 1
        test_utils.assert_latest_refresh_file_record(
            session=global_session,
            repository_path=mdm1.repository_root,
            filepath=file_1,
            error_occured=False,
            lines_added=0,
            lines_removed=0,
            running_lines_added=3,
            running_lines_removed=2,
        )

        # refresh 3 file 3
        test_utils.assert_latest_refresh_file_record(
            session=global_session,
            repository_path=mdm1.repository_root,
            filepath=file_3,
            error_occured=True,
        )

        # REFRESH 4:
        #   - delete file 1 using uring manager method
        with LocalSessionOrExit(db_path=mdm1.db_path) as local_session:
            mdm1.remove_file(session=local_session, filepath=file_1)

        global_manager.refresh_all_repositories(session=global_session)

        # refresh 4 repo 1
        test_utils.assert_latest_refresh_repository_record(
            session=global_session,
            path=mdm1.repository_root,
            error_occured=False,
            total_files=1,
            files_refreshed=0,
            files_failed=1,
        )

        # refresh 4 file 1
        assert not test_utils.get_latest_refresh_file_record(
            session=global_session, repository_path=working_dir, filepath=file_1
        )


@pytest.mark.b4ce471352
@pytest.mark.global_
@pytest.mark.refresh
@pytest.mark.sanity
def test_global_manager_overwrite_refresh_statisitcs_for_files_replaced_by_touch(
    mdm_config, working_dir, global_manager
):
    """
    Testing scenario when file is manually removed and then replaced
    by another file with the same name. From the perspective of refresh
    tracking, this should be treated as the same file but the statistics
    are reset from the point when files was replaced because becase 'touch'
    resets the file history.
    """

    file_1 = working_dir.joinpath("file_1")
    mdm = MetadataManager.new(md_config=mdm_config, path=working_dir)

    with LocalSessionOrExit(db_path=mdm.db_path) as local_session:
        mdm.touch(session=local_session, filepath=file_1)
        file_1.write_text("one\ntwo\nthree")

    with GlobalSessionOrExit(db_path=global_manager.db_path) as global_session:
        global_manager.refresh_all_repositories(session=global_session)

        test_utils.assert_latest_refresh_file_record(
            session=global_session,
            repository_path=mdm.repository_root,
            filepath=file_1,
            error_occured=False,
            lines_added=3,
            lines_removed=0,
            running_lines_added=3,
            running_lines_removed=0,
        )

        file_1.unlink()
        global_manager.refresh_all_repositories(session=global_session)

        test_utils.assert_latest_refresh_file_record(
            session=global_session,
            repository_path=mdm.repository_root,
            filepath=file_1,
            error_occured=True,
        )

        with LocalSessionOrExit(db_path=mdm.db_path) as local_session:
            mdm.touch(session=local_session, filepath=file_1)
            file_1.write_text("one\ntwo")

        global_manager.refresh_all_repositories(session=global_session)

        test_utils.assert_latest_refresh_file_record(
            session=global_session,
            repository_path=mdm.repository_root,
            filepath=file_1,
            error_occured=False,
            lines_added=2,
            lines_removed=0,
            running_lines_added=2,
            running_lines_removed=0,
        )


@pytest.mark.d65f69ab74
@pytest.mark.global_
@pytest.mark.refresh
@pytest.mark.sanity
def test_global_manager_doesnt_overwrite_refresh_statisitcs_for_files_replaced_by_add(
    mdm_config, working_dir, global_manager
):
    """
    Testing scenario when file is manually removed and then replaced
    by another file with the same name. From the perspective of refresh
    tracking, this should be treated as the same file and statistics are
    not reset when 'add' command was used to start tracking the file.
    """

    file_1 = working_dir.joinpath("file_1")
    mdm = MetadataManager.new(md_config=mdm_config, path=working_dir)

    with LocalSessionOrExit(db_path=mdm.db_path) as local_session:
        mdm.touch(session=local_session, filepath=file_1)
        file_1.write_text("one\ntwo\nthree")

    with GlobalSessionOrExit(db_path=global_manager.db_path) as global_session:
        global_manager.refresh_all_repositories(session=global_session)

        test_utils.assert_latest_refresh_file_record(
            session=global_session,
            repository_path=mdm.repository_root,
            filepath=file_1,
            error_occured=False,
            lines_added=3,
            lines_removed=0,
            running_lines_added=3,
            running_lines_removed=0,
        )

        file_1.unlink()
        global_manager.refresh_all_repositories(session=global_session)

        test_utils.assert_latest_refresh_file_record(
            session=global_session,
            repository_path=mdm.repository_root,
            filepath=file_1,
            error_occured=True,
        )

        with LocalSessionOrExit(db_path=mdm.db_path) as local_session:
            file_1.write_text("one\ntwo\n")
            mdm.add_file(session=local_session, filepath=file_1)

        global_manager.refresh_all_repositories(session=global_session)

        test_utils.assert_latest_refresh_file_record(
            session=global_session,
            repository_path=mdm.repository_root,
            filepath=file_1,
            error_occured=False,
            lines_added=0,
            lines_removed=1,
            running_lines_added=3,
            running_lines_removed=1,
        )


@pytest.mark.ebd0b99e11
@pytest.mark.global_
@pytest.mark.refresh
@pytest.mark.sanity
def test_refresh_produces_logs(working_dir, mdm_config, global_manager):
    mdm = MetadataManager.new(md_config=mdm_config, path=working_dir)

    file_1 = working_dir.joinpath("file_1")

    with LocalSessionOrExit(db_path=mdm.db_path) as local_session:
        mdm.touch(session=local_session, filepath=file_1)

    with GlobalSessionOrExit(db_path=global_manager.db_path) as global_session:
        global_manager.refresh_all_repositories(session=global_session)

    assert mdm_config.get_global_log_path().exists()

    info_log = mdm_config.get_global_info_log_filepath()
    debug_log = mdm_config.get_global_debug_log_filepath()
    assert info_log.exists()
    assert debug_log.exists()

    assert len(info_log.read_text()) != 0
    assert len(debug_log.read_text()) != 0

    assert "traceback" not in info_log.read_text().lower()
    assert "traceback" not in debug_log.read_text().lower()


@pytest.mark.e60e75540e
@pytest.mark.global_
@pytest.mark.refresh
@pytest.mark.sanity
def test_global_refresh_logs_traceback_on_file_level_failure(
    working_dir, mdm_config, global_manager
):
    mdm = MetadataManager.new(md_config=mdm_config, path=working_dir)

    file_1 = working_dir.joinpath("file_1")

    with LocalSessionOrExit(db_path=mdm.db_path) as local_session:
        mdm.touch(session=local_session, filepath=file_1)

    # remove the file
    file_1.unlink()

    with GlobalSessionOrExit(db_path=global_manager.db_path) as global_session:
        global_manager.refresh_all_repositories(session=global_session)

    assert mdm_config.get_global_log_path().exists()

    info_log = mdm_config.get_global_info_log_filepath()
    debug_log = mdm_config.get_global_debug_log_filepath()

    assert "traceback" in info_log.read_text().lower()
    assert "traceback" in debug_log.read_text().lower()


@pytest.mark.bfa38e859b
@pytest.mark.global_
@pytest.mark.refresh
@pytest.mark.sanity
def test_global_refresh_logs_traceback_on_repository_level_error(
    working_dir, mdm_config, global_manager
):
    mdm = MetadataManager.new(md_config=mdm_config, path=working_dir)

    # corrupt the database
    test_utils.corrupt_sqlite_file(path=mdm.db_path)

    with GlobalSessionOrExit(db_path=global_manager.db_path) as global_session:
        global_manager.refresh_all_repositories(session=global_session)

    info_log = mdm_config.get_global_info_log_filepath()
    debug_log = mdm_config.get_global_debug_log_filepath()

    assert "traceback" in info_log.read_text().lower()
    assert "traceback" in debug_log.read_text().lower()


@pytest.mark.d01456a09e
@pytest.mark.global_
@pytest.mark.refresh
@pytest.mark.sanity
def test_global_refresh_info_log_is_subset_of_debug_log(
    working_dir, mdm_config, global_manager
):
    file_1 = working_dir.joinpath("file_1")
    file_2 = working_dir.joinpath("file_2")
    mdm = MetadataManager.new(md_config=mdm_config, path=working_dir)

    with LocalSessionOrExit(db_path=mdm.db_path) as local_session:
        mdm.touch(session=local_session, filepath=file_1)
        mdm.touch(session=local_session, filepath=file_2)

        with GlobalSessionOrExit(db_path=global_manager.db_path) as global_session:
            global_manager.refresh_all_repositories(session=global_session)

        mdm.remove_file(session=local_session, filepath=file_1)
        mdm.touch(session=local_session, filepath=working_dir.joinpath("file_3"))

        with GlobalSessionOrExit(db_path=global_manager.db_path) as global_session:
            global_manager.refresh_all_repositories(session=global_session)

    info_log = mdm_config.get_global_info_log_filepath()
    debug_log = mdm_config.get_global_debug_log_filepath()

    with open(info_log, "r") as info_log_file:
        debug_text = debug_log.read_text()

        for line in info_log_file:
            assert line in debug_text


@pytest.mark.dfdcb4e122
@pytest.mark.global_
@pytest.mark.refresh
@pytest.mark.sanity
def test_global_refresh_appends_to_log_files(working_dir, mdm_config, global_manager):
    file_1 = working_dir.joinpath("file_1")
    mdm = MetadataManager.new(md_config=mdm_config, path=working_dir)

    info_log = mdm_config.get_global_info_log_filepath()
    debug_log = mdm_config.get_global_debug_log_filepath()

    with LocalSessionOrExit(db_path=mdm.db_path) as local_session:
        mdm.touch(session=local_session, filepath=file_1)

    with GlobalSessionOrExit(db_path=global_manager.db_path) as global_session:
        global_manager.refresh_all_repositories(session=global_session)

    info_text = info_log.read_text()
    debug_text = debug_log.read_text()

    with GlobalSessionOrExit(db_path=global_manager.db_path) as global_session:
        global_manager.refresh_all_repositories(session=global_session)

    updated_info_text = info_log.read_text()
    updated_debug_text = debug_log.read_text()

    assert debug_text in updated_debug_text
    assert len(updated_debug_text) > len(debug_text)

    assert info_text in updated_info_text
    assert len(updated_info_text) > len(info_text)
