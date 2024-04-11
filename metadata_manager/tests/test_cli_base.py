import pytest
import subprocess

from manager import MetadataManager
from models.local_models import FileORM
from md_enums import FileStatus
from db import get_session_or_exit

#######################################################################
# Mdm commands can't be run outside of Mdm repository.                #
#######################################################################


@pytest.mark.a47d333574
@pytest.mark.cli
@pytest.mark.base
@pytest.mark.untrack
@pytest.mark.sanity
def test_untrack_cant_be_used_outside_of_mdm_repository(
    working_dir, mdm_config, monkeypatch, untrack_cmd
):
    repository_root = working_dir.joinpath("dir1")
    repository_root.mkdir()

    filepath = repository_root.joinpath("testfile")

    # Initialize Mdm outside outside of cwd.
    mdm = MetadataManager.new(md_config=mdm_config, path=repository_root)
    session = get_session_or_exit(db_path=mdm.db_path)
    mdm.touch(session=session, filepath=filepath)

    proc = subprocess.run([*untrack_cmd, filepath], capture_output=True)
    assert proc.returncode == 100

    monkeypatch.chdir(repository_root)
    subprocess.check_output([*untrack_cmd, filepath])

    assert session.query(FileORM).filter_by(status=FileStatus.UNTRACKED).first()

    session.close()


@pytest.mark.c5c9342acb
@pytest.mark.cli
@pytest.mark.base
@pytest.mark.purge
@pytest.mark.sanity
def test_purge_cant_be_used_outside_of_mdm_repository(
    working_dir, mdm_config, monkeypatch, purge_cmd
):
    repository_root = working_dir.joinpath("dir1")
    repository_root.mkdir()

    filepath = repository_root.joinpath("testfile")

    # Initialize Mdm outside outside of cwd.
    mdm = MetadataManager.new(md_config=mdm_config, path=repository_root)
    session = get_session_or_exit(db_path=mdm.db_path)
    mdm.touch(session=session, filepath=filepath)
    session.close()

    proc = subprocess.run([*purge_cmd])
    assert proc.returncode == 100

    monkeypatch.chdir(repository_root)
    subprocess.check_output([*purge_cmd])


@pytest.mark.add1d69a33
@pytest.mark.cli
@pytest.mark.base
@pytest.mark.list
@pytest.mark.sanity
def test_list_cant_be_used_outside_of_mdm_repository(
    working_dir, mdm_config, monkeypatch, list_cmd
):
    repository_root = working_dir.joinpath("dir1")
    repository_root.mkdir()

    filepath = repository_root.joinpath("testfile")

    # Initialize Mdm outside outside of cwd.
    mdm = MetadataManager.new(md_config=mdm_config, path=repository_root)
    session = get_session_or_exit(db_path=mdm.db_path)
    mdm.touch(session=session, filepath=filepath)
    session.close()

    proc = subprocess.run([*list_cmd])
    assert proc.returncode == 100

    monkeypatch.chdir(repository_root)
    proc = subprocess.run([*list_cmd], capture_output=True)
    assert proc.returncode == 0
    assert "testfile" in proc.stdout.decode().lower()


@pytest.mark.f7d29d3a6b
@pytest.mark.cli
@pytest.mark.base
@pytest.mark.touch
@pytest.mark.sanity
def test_touch_cant_be_used_outside_of_mdm_repository(
    working_dir, mdm_config, monkeypatch, touch_cmd
):
    repository_root = working_dir.joinpath("dir1")
    repository_root.mkdir()
    subdir = repository_root.joinpath("dir2")
    subdir.mkdir()

    filepath1 = repository_root.joinpath("testfile1")
    filepath2 = subdir.joinpath("testfile2")

    # Initialize Mdm outside outside of cwd.
    MetadataManager.new(md_config=mdm_config, path=repository_root)
    proc = subprocess.run([*touch_cmd, filepath1])
    assert proc.returncode == 100
    proc = subprocess.run([*touch_cmd, filepath2])
    assert proc.returncode == 100
    assert not filepath1.exists()
    assert not filepath2.exists()

    # Change cwd to Mdm repository root.
    monkeypatch.chdir(repository_root)
    subprocess.check_output([*touch_cmd, filepath1])
    subprocess.check_output([*touch_cmd, filepath2])
    assert filepath1.exists()
    assert filepath2.exists()


@pytest.mark.e26c08024c
@pytest.mark.cli
@pytest.mark.base
@pytest.mark.rm
@pytest.mark.sanity
def test_rm_cant_be_used_outside_of_mdm_repository(
    working_dir, rm_cmd, mdm_config, monkeypatch
):
    repository_root = working_dir.joinpath("dir1")
    repository_root.mkdir()
    subdir = repository_root.joinpath("dir2")
    subdir.mkdir()

    filepath1 = repository_root.joinpath("testfile1")
    filepath2 = subdir.joinpath("testfile2")

    mdm = MetadataManager.new(md_config=mdm_config, path=repository_root)
    session = get_session_or_exit(db_path=mdm.db_path)
    mdm.touch(session=session, filepath=filepath1)
    mdm.touch(session=session, filepath=filepath2)
    session.close()

    # These operations should fail since we are switching away from mdm repository.
    proc = subprocess.run([*rm_cmd, filepath1], capture_output=True)
    assert proc.returncode == 100
    proc = subprocess.run([*rm_cmd, filepath2], capture_output=True)
    assert proc.returncode == 100
    assert filepath1.exists()
    assert filepath2.exists()

    # Now they should work correctly.
    monkeypatch.chdir(repository_root)
    subprocess.check_output([*rm_cmd, filepath1])
    subprocess.check_output([*rm_cmd, filepath2])
    assert not filepath1.exists()
    assert not filepath2.exists()
