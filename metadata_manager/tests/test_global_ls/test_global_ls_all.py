import pytest
import subprocess
import shutil

from manager import MetadataManager


@pytest.mark.c2a615d66f
@pytest.mark.cli
@pytest.mark.global_
@pytest.mark.ls
@pytest.mark.all
@pytest.mark.sanity
def test_global_ls_displays_only_valid_repos_by_default(
    working_dir, mdm_config, global_ls_cmd
):
    dir_ = working_dir.joinpath("dir_")
    dir_.mkdir()

    MetadataManager.new(md_config=mdm_config, path=working_dir)
    MetadataManager.new(md_config=mdm_config, path=dir_)

    shutil.rmtree(dir_)

    output = subprocess.check_output([*global_ls_cmd])

    assert str(working_dir) in output.decode()
    assert str(dir_) not in output.decode()


@pytest.mark.c38db44470
@pytest.mark.cli
@pytest.mark.global_
@pytest.mark.ls
@pytest.mark.all
@pytest.mark.sanity
def test_global_ls_displays_all_repositories(working_dir, mdm_config, global_ls_cmd):
    dir_ = working_dir.joinpath("dir_")
    dir_.mkdir()

    MetadataManager.new(md_config=mdm_config, path=working_dir)
    MetadataManager.new(md_config=mdm_config, path=dir_)

    shutil.rmtree(dir_)

    output = subprocess.check_output([*global_ls_cmd, "--all"])

    assert str(working_dir) in output.decode()
    assert str(dir_) in output.decode()
