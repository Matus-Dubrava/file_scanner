import pytest
from pathlib import Path
import shutil
import os


@pytest.fixture(scope="function")
def working_dir():
    working_dir_path = Path(__file__).absolute().parent / "working_dir"
    if working_dir_path.exists():
        shutil.rmtree(working_dir_path)

    os.makedirs(working_dir_path)
    yield working_dir_path
    # shutil.rmtree(working_dir_path)


@pytest.fixture(scope="module")
def md_cmd():
    manager_path = Path(__file__).parent.parent / "cli.py"
    yield ["python3", manager_path]


@pytest.fixture(scope="module")
def init_cmd(md_cmd):
    yield [*md_cmd, "init"]
