import pytest
import subprocess

from manager import MetadataManager


@pytest.mark.dab95ab2d8
@pytest.mark.cli
@pytest.mark.global_
@pytest.mark.ls
@pytest.mark.sanity
def test_global_ls_lists_all_repositories(working_dir, mdm_config, global_ls_cmd):
    dir_ = working_dir.joinpath("dir_")
    dir_.mkdir()

    MetadataManager.new(md_config=mdm_config, path=working_dir)
    MetadataManager.new(md_config=mdm_config, path=dir_)

    output = subprocess.check_output([*global_ls_cmd])

    assert str(working_dir) in output.decode()
    assert str(dir_) in output.decode()


@pytest.mark.ac27da190e
@pytest.mark.cli
@pytest.mark.global_
@pytest.mark.ls
@pytest.mark.sanity
def test_global_ls_works_without_any_repository_records(
    working_dir, mdm_config, global_ls_cmd
):
    subprocess.check_output([*global_ls_cmd])
