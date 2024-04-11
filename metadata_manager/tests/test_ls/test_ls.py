import pytest

from md_enums import FileStatus
from models.local_models import FileORM


@pytest.mark.a06e4f2a96
@pytest.mark.ls
@pytest.mark.manager
@pytest.mark.sanity
def test_list_files_filters_based_on_file_status(working_dir, mdm, session):
    subrepo_path = working_dir.joinpath("subdir")
    subrepo_path.mkdir()

    active1 = working_dir.joinpath("active1")
    active2 = working_dir.joinpath("active2")
    removed1 = working_dir.joinpath("removed1")
    untracked1 = working_dir.joinpath("untracked1")
    untracked2 = working_dir.joinpath("untracked2")
    subrepo_tracked = subrepo_path.joinpath("subrepo_tracked")

    mdm.touch(session=session, filepath=active1)
    mdm.touch(session=session, filepath=active2)
    mdm.touch(session=session, filepath=removed1)
    mdm.touch(session=session, filepath=untracked1)
    mdm.touch(session=session, filepath=untracked2)
    mdm.touch(session=session, filepath=subrepo_tracked)

    mdm.remove_file(session=session, filepath=removed1)
    mdm.untrack(session=session, filepath=untracked1)
    mdm.untrack(session=session, filepath=untracked2)

    session.query(FileORM).filter_by(filepath=subrepo_tracked).update(
        {"status": FileStatus.TRACKED_IN_SUBREPOSITORY}
    )

    status_filter = [FileStatus.ACTIVE]
    assert sorted(
        [
            str(record.filepath)
            for record in mdm._list_files(session=session, status_filter=status_filter)
        ]
    ) == sorted([str(active1), str(active2)])

    status_filter = [FileStatus.UNTRACKED]
    assert sorted(
        [
            str(record.filepath)
            for record in mdm._list_files(session=session, status_filter=status_filter)
        ]
    ) == sorted([str(untracked1), str(untracked2)])

    status_filter = [FileStatus.TRACKED_IN_SUBREPOSITORY]
    assert sorted(
        [
            str(record.filepath)
            for record in mdm._list_files(session=session, status_filter=status_filter)
        ]
    ) == sorted([str(subrepo_tracked)])

    # Filepaths of removed files are mangled.
    status_filter = [FileStatus.REMOVED]
    result = [
        str(record.filepath)
        for record in mdm._list_files(session=session, status_filter=status_filter)
    ]
    assert len(result) == 1

    status_filter = [FileStatus.ACTIVE, FileStatus.UNTRACKED]
    assert sorted(
        [
            str(record.filepath)
            for record in mdm._list_files(session=session, status_filter=status_filter)
        ]
    ) == sorted([str(active1), str(active2), str(untracked1), str(untracked2)])
