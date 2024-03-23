import pytest
from pathlib import Path
import shutil
import os

from manager import MetadataManager
from md_models import Config
from db import get_session


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


@pytest.fixture(scope="function")
def working_dir(request, monkeypatch, md_manager):
    should_init_md = request.node.get_closest_marker("init_md")

    working_dir_path = Path("/tmp/working_dir/")

    if working_dir_path.exists():
        shutil.rmtree(working_dir_path)

    os.makedirs(working_dir_path)
    print(f"[{__name__}] created working dir: {working_dir_path}")
    monkeypatch.chdir(working_dir_path)
    print(f"[{__name__}] changed CWD to: {working_dir_path}")

    if should_init_md and should_init_md.args[0]:
        md_manager.initalize_md_repository(working_dir_path)
        print(
            f"[{__name__}] initialized md repository: {working_dir_path.joinpath(md_manager.md_config.md_dir_name)}"
        )

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


@pytest.fixture(scope="function")
def md_manager():
    with open(Path(__file__).parent / "config" / ".mdconfig_dev", "r") as f:
        md_config = Config.model_validate_json(f.read())

    return MetadataManager(md_config)


@pytest.fixture(scope="function")
@pytest.mark.init_md(True)
def session(working_dir, md_manager):
    sess = get_session(
        working_dir.joinpath(
            md_manager.md_config.md_dir_name, md_manager.md_config.md_db_name
        )
    )
    print(f"[{__name__}] created session object")
    yield sess
    sess.close()
