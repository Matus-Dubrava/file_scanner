import pytest
from pathlib import Path
import shutil
import os

from manager import MetadataManager
from md_models import Config
from db import get_session_or_exit


def pytest_runtest_setup(item):
    if "preserve_version_data" in item.keywords:
        item.session.version_file_path = Path(__file__).parent.joinpath("version.json")
        version_data = ""
        with open(item.session.version_file_path, "rb") as bf:
            version_data = bf.read()
            item.session.version_data = version_data


def pytest_runtest_teardown(item):
    if "preserve_version_data" in item.keywords:
        with open(item.session.version_file_path, "wb") as bf:
            bf.write(item.session.version_data)


@pytest.fixture(scope="module")
def working_dir_path():
    path = Path("/tmp/working_dir/")
    return path


@pytest.fixture(scope="function")
def working_dir(working_dir_path, monkeypatch):
    if working_dir_path.exists():
        shutil.rmtree(working_dir_path)

    os.makedirs(working_dir_path)
    print(f"[{__name__}] created working dir: {working_dir_path}")
    monkeypatch.chdir(working_dir_path)
    print(f"[{__name__}] changed CWD to: {working_dir_path}")

    yield working_dir_path
    shutil.rmtree(working_dir_path)


@pytest.fixture(scope="module")
def md_cmd():
    manager_path = Path(__file__).parent / "cli.py"
    return ["python3", manager_path]


@pytest.fixture(scope="module")
def init_cmd(md_cmd):
    return [*md_cmd, "init"]


@pytest.fixture(scope="module")
def touch_cmd(md_cmd):
    return [*md_cmd, "touch"]


@pytest.fixture(scope="module")
def untrack_cmd(md_cmd):
    return [*md_cmd, "untrack"]


@pytest.fixture(scope="module")
def rm_cmd(md_cmd):
    return [*md_cmd, "rm"]


@pytest.fixture(scope="module")
def purge_cmd(md_cmd):
    return [*md_cmd, "purge"]


@pytest.fixture(scope="module")
def list_cmd(md_cmd):
    return [*md_cmd, "ls"]


@pytest.fixture(scope="module")
def add_cmd(md_cmd):
    return [*md_cmd, "add"]


@pytest.fixture(scope="module")
def refresh_cmd(md_cmd):
    return [*md_cmd, "refresh"]


@pytest.fixture(scope="module")
def show_cmd(md_cmd):
    return [*md_cmd, "show"]


@pytest.fixture(scope="module")
def setv_cmd(md_cmd):
    return [*md_cmd, "setv"]


@pytest.fixture(scope="function")
def mdm_config():
    with open(Path(__file__).parent / "config" / ".mdconfig_dev", "r") as f:
        md_config = Config.model_validate_json(f.read())
        return md_config


@pytest.fixture(scope="function")
def mdm(working_dir, mdm_config):
    # NOTE UPDATE: the below issue has been fixed by not reusing session object extensively
    # throughout the manager instance. Most likely cause was closing the same session multiple
    # times and adding/commiting to closed session. Interestingly enough, this seemed to be an
    # issue only while running tests.
    #
    # NOTE ORIGINAL: There seems to be an issue when using 'DELETE' journal mode while running tests.
    # It seems like some race condition - sometimes something is not cleaned up properly
    # which leads to the following error:
    #
    #   (sqlite3.OperationalError) disk I/O error
    #
    # This happens even when this fixture's scope is function and each test case that is using
    # it should be working in clean 'working_dir' and have its own session.
    # mdm.session.execute(text("PRAGMA journal_mode=OFF"))
    # print(
    #     f"SQLITE jounal mode: {mdm.session.execute(text('PRAGMA journal_mode')).fetchall()}"
    # )
    return MetadataManager.new(md_config=mdm_config, path=working_dir)


@pytest.fixture(scope="function")
def session(mdm):
    session_ = get_session_or_exit(db_path=mdm.db_path)
    print(f"[{__name__}] session established, database: {mdm.db_path}")
    yield session_
    session_.close()
