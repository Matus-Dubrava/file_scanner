import pytest
import subprocess

from md_models import FileORM
from md_enums import FileStatus


@pytest.mark.fd83c0e100
@pytest.mark.cli
@pytest.mark.ls
@pytest.mark.sanity
def test_ls_displays_active_files_by_default(working_dir, mdm, list_cmd):
    subrepo_path = working_dir.joinpath("subdir")
    subrepo_path.mkdir()

    active1 = working_dir.joinpath("active1")
    active2 = working_dir.joinpath("active2")
    removed1 = working_dir.joinpath("removed1")
    untracked1 = working_dir.joinpath("untracked1")
    untracked2 = working_dir.joinpath("untracked2")
    subrepo_tracked = subrepo_path.joinpath("subrepo_tracked")

    mdm.touch(active1)
    mdm.touch(active2)
    mdm.touch(removed1)
    mdm.touch(untracked1)
    mdm.touch(untracked2)
    mdm.touch(subrepo_tracked)

    mdm.remove_file(removed1)
    mdm.untrack(untracked1)
    mdm.untrack(untracked2)

    mdm.session.query(FileORM).filter_by(filepath=subrepo_tracked).update(
        {"status": FileStatus.TRACKED_IN_SUBREPOSITORY}
    )
    mdm.session.commit()

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
def test_ls_accepts_status_filter_flags(working_dir, mdm, list_cmd):
    subrepo_path = working_dir.joinpath("subdir")
    subrepo_path.mkdir()

    active1 = working_dir.joinpath("active1")
    active2 = working_dir.joinpath("active2")
    removed1 = working_dir.joinpath("removed1")
    untracked1 = working_dir.joinpath("untracked1")
    untracked2 = working_dir.joinpath("untracked2")
    subrepo_tracked = subrepo_path.joinpath("subrepo_tracked")

    mdm.touch(active1)
    mdm.touch(active2)
    mdm.touch(removed1)
    mdm.touch(untracked1)
    mdm.touch(untracked2)
    mdm.touch(subrepo_tracked)

    mdm.remove_file(removed1)
    mdm.untrack(untracked1)
    mdm.untrack(untracked2)

    mdm.session.query(FileORM).filter_by(filepath=subrepo_tracked).update(
        {"status": FileStatus.TRACKED_IN_SUBREPOSITORY}
    )
    mdm.session.commit()

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
