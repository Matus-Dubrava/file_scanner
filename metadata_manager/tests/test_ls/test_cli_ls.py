import pytest
import subprocess

from models.local_models import FileORM
from md_enums import FileStatus


@pytest.mark.fd83c0e100
@pytest.mark.cli
@pytest.mark.ls
@pytest.mark.sanity
def test_ls_displays_active_files_by_default(working_dir, mdm, list_cmd, session):
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
    session.commit()

    proc = subprocess.run([*list_cmd], capture_output=True)
    assert proc.returncode == 0

    assert str(active1.name) in proc.stdout.decode().strip()
    assert str(active2.name) in proc.stdout.decode().strip()
    assert str(removed1.name) not in proc.stdout.decode().strip()
    assert str(untracked1.name) not in proc.stdout.decode().strip()
    assert str(untracked2.name) not in proc.stdout.decode().strip()
    assert str(subrepo_tracked.name) not in proc.stdout.decode().strip()


@pytest.mark.c464f2769a
@pytest.mark.cli
@pytest.mark.ls
@pytest.mark.sanity
def test_ls_accepts_status_filter_flags(working_dir, mdm, list_cmd, session):
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
    session.commit()

    proc = subprocess.run([*list_cmd, "--active"], capture_output=True)
    assert proc.returncode == 0

    assert str(active1.name) in proc.stdout.decode().strip()
    assert str(active2.name) in proc.stdout.decode().strip()
    assert str(removed1.name) not in proc.stdout.decode().strip()
    assert str(untracked1.name) not in proc.stdout.decode().strip()
    assert str(untracked2.name) not in proc.stdout.decode().strip()
    assert str(subrepo_tracked.name) not in proc.stdout.decode().strip()

    proc = subprocess.run([*list_cmd, "--untracked"], capture_output=True)
    assert proc.returncode == 0

    assert str(active1.name) not in proc.stdout.decode().strip()
    assert str(active2.name) not in proc.stdout.decode().strip()
    assert str(removed1.name) not in proc.stdout.decode().strip()
    assert str(untracked1.name) in proc.stdout.decode().strip()
    assert str(untracked2.name) in proc.stdout.decode().strip()
    assert str(subrepo_tracked.name) not in proc.stdout.decode().strip()

    proc = subprocess.run([*list_cmd, "--subrepository-tracked"], capture_output=True)
    assert proc.returncode == 0

    assert str(active1.name) not in proc.stdout.decode().strip()
    assert str(active2.name) not in proc.stdout.decode().strip()
    assert str(removed1.name) not in proc.stdout.decode().strip()
    assert str(untracked1.name) not in proc.stdout.decode().strip()
    assert str(untracked2.name) not in proc.stdout.decode().strip()
    assert str(subrepo_tracked.name) in proc.stdout.decode().strip()

    proc = subprocess.run([*list_cmd, "--removed"], capture_output=True)
    assert proc.returncode == 0

    assert str(active1.name) not in proc.stdout.decode().strip()
    assert str(active2.name) not in proc.stdout.decode().strip()
    assert str(removed1.name) in proc.stdout.decode().strip()
    assert str(untracked1.name) not in proc.stdout.decode().strip()
    assert str(untracked2.name) not in proc.stdout.decode().strip()
    assert str(subrepo_tracked.name) not in proc.stdout.decode().strip()

    proc = subprocess.run([*list_cmd, "--untracked", "--active"], capture_output=True)
    assert proc.returncode == 0

    assert str(active1.name) in proc.stdout.decode().strip()
    assert str(active2.name) in proc.stdout.decode().strip()
    assert str(removed1.name) not in proc.stdout.decode().strip()
    assert str(untracked1.name) in proc.stdout.decode().strip()
    assert str(untracked2.name) in proc.stdout.decode().strip()
    assert str(subrepo_tracked.name) not in proc.stdout.decode().strip()

    proc = subprocess.run([*list_cmd, "--all"], capture_output=True)
    assert proc.returncode == 0

    assert str(active1.name) in proc.stdout.decode().strip()
    assert str(active2.name) in proc.stdout.decode().strip()
    assert str(removed1.name) in proc.stdout.decode().strip()
    assert str(untracked1.name) in proc.stdout.decode().strip()
    assert str(untracked2.name) in proc.stdout.decode().strip()
    assert str(subrepo_tracked.name) in proc.stdout.decode().strip()
