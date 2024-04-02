import pytest
import subprocess

from md_models import FileListing, RepositoryORM
from md_enums import FileStatus


@pytest.mark.ec2dfa9966
@pytest.mark.cli
@pytest.mark.ls
@pytest.mark.dump_json
@pytest.mark.sanity
def test_ls_dumps_json_data_to_file(working_dir, mdm, list_cmd):
    subdir = working_dir.joinpath("subdir")
    subdir.mkdir()
    testfile1 = working_dir.joinpath("testfile1")
    testfile2 = working_dir.joinpath("testfile2")
    testfile3 = subdir.joinpath("testfile3")

    mdm.touch(testfile1, debug=True)
    mdm.touch(testfile2, debug=True)
    mdm.touch(testfile3, debug=True)
    mdm.remove_file(testfile2)
    mdm.untrack(testfile3)

    repository_record = mdm.session.query(RepositoryORM).first()
    assert repository_record

    status_filter = [FileStatus.ACTIVE.to_str(), FileStatus.UNTRACKED.to_str()]
    json_path = working_dir.joinpath("result.json")

    subprocess.check_output(
        [*list_cmd, "--dump-json", json_path, "--active", "--untracked"]
    )

    file_listing = FileListing.model_validate_json(json_path.read_text())
    assert file_listing.repository_id == repository_record.id
    assert file_listing.repository_path == repository_record.repository_filepath
    assert sorted(
        [status.to_str() for status in file_listing.applied_status_filters]
    ) == sorted(status_filter)
    assert sorted([str(path) for path in file_listing.filepaths]) == sorted(
        [str(testfile1), str(testfile3)]
    )


@pytest.mark.b6f97c83f5
@pytest.mark.cli
@pytest.mark.ls
@pytest.mark.dump_json
@pytest.mark.sanity
def test_ls_prints_noting_to_stdout_when_dumping_to_json(working_dir, mdm, list_cmd):
    testfile1 = working_dir.joinpath("testfile1")
    mdm.touch(testfile1)

    json_path = working_dir.joinpath("result.json")
    res = subprocess.check_output([*list_cmd, "--dump-json", json_path])
    assert not res


@pytest.mark.ca23a34954
@pytest.mark.cli
@pytest.mark.ls
@pytest.mark.dump_json
@pytest.mark.sanity
def test_ls_fails_to_dump_json_if_parent_directory_doesnt_exist(
    working_dir, mdm, list_cmd
):
    testfile1 = working_dir.joinpath("testfile1")
    mdm.touch(testfile1)

    json_path = working_dir.joinpath("dir1", "result.json")

    proc = subprocess.run([*list_cmd, "--dump-json", json_path], capture_output=True)
    assert proc.returncode == 1

    assert "fatal" in proc.stderr.decode().lower()
    assert str(json_path) in proc.stderr.decode()
    assert not proc.stdout

    assert "traceback" not in proc.stderr.decode().lower()


@pytest.mark.ae448a920f
@pytest.mark.cli
@pytest.mark.ls
@pytest.mark.dump_json
@pytest.mark.sanity
def test_debug_option_prints_traceback_when_dumping_to_json_fails(
    working_dir, mdm, list_cmd
):
    testfile1 = working_dir.joinpath("testfile1")
    mdm.touch(testfile1)

    json_path = working_dir.joinpath("dir1", "result.json")

    proc = subprocess.run(
        [*list_cmd, "--dump-json", json_path, "--debug"], capture_output=True
    )
    assert proc.returncode == 1
    assert str(json_path) in proc.stderr.decode()
    assert not proc.stdout

    assert "traceback" in proc.stderr.decode().lower()


@pytest.mark.ebdc1092de
@pytest.mark.cli
@pytest.mark.ls
@pytest.mark.dump_json
@pytest.mark.sanity
@pytest.mark.parametrize("force_flag", ["-f", "--force"])
def test_force_flag_creates_parent_directories_when_dumping_to_json(
    working_dir, mdm, list_cmd, force_flag
):
    testfile1 = working_dir.joinpath("testfile1")
    mdm.touch(testfile1)

    json_path = working_dir.joinpath("dir1", "dir2", "result.json")

    subprocess.check_output([*list_cmd, "--dump-json", json_path, force_flag])

    assert json_path.exists()
    FileListing.model_validate_json(json_path.read_text())
