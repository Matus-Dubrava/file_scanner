import pytest
import subprocess

import md_constants
from manager import MetadataManager


@pytest.mark.cc11134814
@pytest.mark.cli
@pytest.mark.ls
@pytest.mark.repository_path
@pytest.mark.sanity
def test_ls_fails_when_run_outside_of_mdm_repository(working_dir, list_cmd):
    with pytest.raises(subprocess.CalledProcessError) as err:
        subprocess.check_output([*list_cmd])

    assert err.value.returncode == md_constants.NOT_MDM_REPOSITORY


@pytest.mark.fe37ff0373
@pytest.mark.cli
@pytest.mark.ls
@pytest.mark.repository_path
@pytest.mark.sanity
def test_ls_works_outside_mdm_repository_when_repository_path_is_provided(
    working_dir, list_cmd, mdm_config
):
    subdir = working_dir.joinpath("dir1")
    subdir.mkdir()
    testfile = subdir.joinpath("testfile")
    mdm = MetadataManager.new(md_config=mdm_config, path=subdir)
    mdm.touch(testfile)

    result = subprocess.check_output([*list_cmd, "--repository-path", subdir])
    assert str(testfile.name) in result.decode()


@pytest.mark.a4e618e23f
@pytest.mark.cli
@pytest.mark.ls
@pytest.mark.repository_path
@pytest.mark.sanity
def test_ls_fails_when_provided_repository_path_is_not_mdm_repository(
    working_dir, list_cmd
):
    with pytest.raises(subprocess.CalledProcessError) as err:
        subprocess.check_output([*list_cmd, "--repository-path", working_dir])

    assert err.value.returncode == md_constants.NOT_MDM_REPOSITORY


@pytest.mark.af42660890
@pytest.mark.cli
@pytest.mark.ls
@pytest.mark.repository_path
@pytest.mark.sanity
def test_respoitory_path_list_file_from_target_respository(
    working_dir, list_cmd, mdm_config
):
    subdir1 = working_dir.joinpath("dir1")
    subdir2 = working_dir.joinpath("dir1", "dir2")

    testfile1 = working_dir.joinpath("testfile1")
    testfile2 = subdir1.joinpath("testfile2")
    testfile3 = subdir2.joinpath("testfile3")

    mdm1 = MetadataManager.new(md_config=mdm_config, path=working_dir)
    mdm2 = MetadataManager.new(md_config=mdm_config, path=subdir1)
    mdm3 = MetadataManager.new(md_config=mdm_config, path=subdir2)

    mdm1.touch(testfile1)
    mdm2.touch(testfile2)
    mdm3.touch(testfile3)

    result = subprocess.check_output([*list_cmd, "--repository-path", working_dir])
    assert str(testfile1.name) in result.decode()
    assert str(testfile2.name) not in result.decode()
    assert str(testfile3.name) not in result.decode()

    result = subprocess.check_output([*list_cmd, "--repository-path", subdir1])
    assert str(testfile1.name) not in result.decode()
    assert str(testfile2.name) in result.decode()
    assert str(testfile3.name) not in result.decode()

    result = subprocess.check_output([*list_cmd, "--repository-path", subdir2])
    assert str(testfile1.name) not in result.decode()
    assert str(testfile2.name) not in result.decode()
    assert str(testfile3.name) in result.decode()
