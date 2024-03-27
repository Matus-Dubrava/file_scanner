import pytest
from pathlib import Path
import shutil
import os


from manager import MetadataManager
from md_models import Config


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


@pytest.fixture(scope="function")
def mdm_config():
    with open(Path(__file__).parent / "config" / ".mdconfig_dev", "r") as f:
        md_config = Config.model_validate_json(f.read())
        return md_config


@pytest.fixture(scope="function")
def mdm(working_dir, mdm_config):
    return MetadataManager.new(md_config=mdm_config, path=working_dir)
