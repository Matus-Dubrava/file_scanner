import pytest
from pathlib import Path
import shutil
import os

from manager import MetadataManager
from md_models import Config
from db import get_session


@pytest.fixture(scope="function")
def working_dir():
    print("creating working dir")
    working_dir_path = Path("/tmp/working_dir/")
    if working_dir_path.exists():
        shutil.rmtree(working_dir_path)

    os.makedirs(working_dir_path)
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


@pytest.fixture(scope="function")
def md_manager():
    with open(Path(__file__).parent / "config" / ".mdconfig_dev", "r") as f:
        md_config = Config.model_validate_json(f.read())

    return MetadataManager(md_config)


@pytest.fixture(scope="function")
def initialize_working_dir(working_dir, md_manager):
    md_manager.initalize_md_repository(working_dir)
    return None


@pytest.fixture(scope="function")
def session(working_dir, initialize_working_dir, md_manager):
    sess = get_session(
        working_dir.joinpath(md_manager.md_config.md_dir_name),
        md_manager.md_config.md_db_name,
    )
    yield sess
    sess.close()


@pytest.fixture(scope="function")
def version_data():
    """
    Used to save and restore version data if some
    test case needs to update it.
    """
    version_file_path = Path(__file__).parent.joinpath("version.json")
    version_data_copy = ""
    with open(version_file_path, "rb") as bf:
        version_data_copy = bf.read()

    yield

    with open(version_file_path, "wb") as bf:
        bf.write(version_data_copy)
