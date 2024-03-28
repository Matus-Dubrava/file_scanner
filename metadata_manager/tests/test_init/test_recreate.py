import pytest
import subprocess
from subprocess import CalledProcessError

from manager import MetadataManager
import utils


@pytest.mark.ad00530bfa
@pytest.mark.init
@pytest.mark.init_recreate
@pytest.mark.cli
@pytest.mark.sanity
def test_init_doesnt_create_new_repository_if_one_exists(
    working_dir, mdm_config, init_cmd
):
    mdm = MetadataManager.new(md_config=mdm_config, path=working_dir)
    assert mdm.md_path.exists()

    with pytest.raises(CalledProcessError) as err:
        subprocess.check_output([*init_cmd, working_dir])

    assert err.value.returncode == 2


@pytest.mark.b7e06ddfb0
@pytest.mark.init
@pytest.mark.init_recreate
@pytest.mark.cli
@pytest.mark.sanity
def test_init_recreates_repository(working_dir, mdm_config, init_cmd):
    mdm = MetadataManager.new(md_config=mdm_config, path=working_dir)
    assert mdm.md_path.exists()

    subprocess.check_output([*init_cmd, working_dir, "--recreate"])
    utils.assert_md_structure_exists(where=working_dir, md_config=mdm_config)
    utils.assert_database_structure(
        db_path=working_dir.joinpath(mdm_config.md_dir_name, mdm_config.md_db_name)
    )


@pytest.mark.b023f65f69
@pytest.mark.init
@pytest.mark.init_recreate
@pytest.mark.cli
@pytest.mark.sanity
def test_recreatate_works_even_when_no_repository_exists(
    working_dir, mdm_config, init_cmd
):
    """
    Ensure that providing --recreate flag works even if target directory isn't repository.
    In such case, it is expected that new repository is created.
    """

    subprocess.check_output([*init_cmd, working_dir, "--recreate"])
    utils.assert_md_structure_exists(where=working_dir, md_config=mdm_config)
    utils.assert_database_structure(
        db_path=working_dir.joinpath(mdm_config.md_dir_name, mdm_config.md_db_name)
    )
