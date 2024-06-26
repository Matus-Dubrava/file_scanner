import pytest
import subprocess

from models.local_models import RepositoryORM


@pytest.mark.ea86bfd10c
@pytest.mark.cli
@pytest.mark.ls
@pytest.mark.no_header
@pytest.mark.sanity
def test_ls_prints_header_information_by_default(list_cmd, session):
    repository_record = session.query(RepositoryORM).first()
    assert repository_record

    result = subprocess.check_output([*list_cmd])
    assert str(repository_record.id) in result.decode()
    assert str(repository_record.repository_filepath) in result.decode()


@pytest.mark.ea86bfd10c
@pytest.mark.cli
@pytest.mark.ls
@pytest.mark.no_header
@pytest.mark.sanity
def test_ls_prints_only_file_info_when_no_header_flag_is_set(
    working_dir, mdm, list_cmd, session
):
    filepath1 = working_dir.joinpath("testfile1")
    filepath2 = working_dir.joinpath("testfile2")

    mdm.touch(session=session, filepath=filepath1)
    mdm.touch(session=session, filepath=filepath2)

    repository_record = session.query(RepositoryORM).first()
    assert repository_record

    result = subprocess.check_output([*list_cmd, "--no-header"])
    assert str(repository_record.id) not in result.decode()
    assert str(repository_record.repository_filepath) not in result.decode()

    assert str(filepath1.name) in result.decode()
    assert str(filepath2.name) in result.decode()

    # We are expecting only 2 lines in result.
    assert result.decode().count("\n") == 2
