import pytest
import subprocess
from pathlib import Path

from manager import MetadataManager
import md_constants


@pytest.mark.d08c44ad12
@pytest.mark.cli
@pytest.mark.rm
@pytest.mark.repository_path
@pytest.mark.sanity
def test_rm_works_when_cwd_matches_target_repository(
    working_dir, mdm_config, monkeypatch, rm_cmd
):
    subrepo1_path = working_dir.joinpath("dir1")
    subrepo1_path.mkdir()

    testfile1 = working_dir.joinpath("testfile")
    testfile2 = subrepo1_path.joinpath("testfile")

    mdm1 = MetadataManager.new(md_config=mdm_config, path=working_dir)
    mdm2 = MetadataManager.new(md_config=mdm_config, path=subrepo1_path)

    def _test_task(mdm: MetadataManager, cwd: Path, filepath: Path) -> None:
        monkeypatch.chdir(cwd)
        mdm.touch(filepath.resolve())
        subprocess.check_output([*rm_cmd, filepath])

    _test_task(mdm1, cwd=working_dir, filepath=testfile1)
    _test_task(mdm2, cwd=subrepo1_path, filepath=testfile2)
    _test_task(mdm1, cwd=working_dir, filepath=subrepo1_path.joinpath("..", "testfile"))


@pytest.mark.b6aa5d0e0c
@pytest.mark.cli
@pytest.mark.rm
@pytest.mark.repository_path
@pytest.mark.sanity
def test_rm_is_blocked_when_cwd_doesnt_match_target_repository(
    working_dir, mdm_config, monkeypatch, rm_cmd
):
    subrepo = working_dir.joinpath("subrepo")
    subrepo.mkdir()

    testfile = subrepo.joinpath("testfile")

    MetadataManager.new(md_config=mdm_config, path=working_dir)
    subrepo_mdm = MetadataManager.new(md_config=mdm_config, path=subrepo)

    subrepo_mdm.touch(testfile)

    with pytest.raises(subprocess.CalledProcessError) as err:
        monkeypatch.chdir(working_dir)
        subprocess.check_output([*rm_cmd, testfile])
        assert testfile.exists()

    assert err.value.returncode == md_constants.AMBIGUOUS_REPOSITORY


@pytest.mark.e1c177f8ef
@pytest.mark.cli
@pytest.mark.rm
@pytest.mark.repository_path
@pytest.mark.sanity
def test_repository_path_option_overrides_cwd_and_unblocks_rm(
    working_dir, mdm_config, rm_cmd
):
    subrepo = working_dir.joinpath("subrepo")
    subrepo.mkdir()

    testfile = subrepo.joinpath("testfile")

    MetadataManager.new(md_config=mdm_config, path=working_dir)
    subrepo_mdm = MetadataManager.new(md_config=mdm_config, path=subrepo)

    subrepo_mdm.touch(testfile)

    subprocess.check_output([*rm_cmd, testfile, "--repository-path", testfile.parent])
    assert not testfile.exists()


@pytest.mark.c768c29d8e
@pytest.mark.cli
@pytest.mark.rm
@pytest.mark.repository_path
@pytest.mark.sanity
def test_rm_works_outside_of_mdm_repository_when_repository_path_is_provided(
    working_dir, mdm_config, rm_cmd
):
    subdir = working_dir.joinpath("dir1")
    subdir.mkdir()
    filepath = subdir.joinpath("testfile")

    mdm = MetadataManager.new(md_config=mdm_config, path=subdir)
    mdm.touch(filepath)

    subprocess.check_output([*rm_cmd, filepath, "--repository-path", subdir])


@pytest.mark.e3089123b1
@pytest.mark.cli
@pytest.mark.rm
@pytest.mark.repository_path
@pytest.mark.sanity
def test_rm_fails_if_provided_filepath_is_not_within_repository_path(
    working_dir, mdm_config, rm_cmd
):
    """
    Testing scenarion when --repository-path is provided but the target filepath
    that we are trying to remove is not within repository where the --repository-path
    points to.
    """

    subdir1 = working_dir.joinpath("dir1")
    subdir2 = working_dir.joinpath("dir2")
    subdir1.mkdir()
    subdir2.mkdir()

    testfile = subdir1.joinpath("testfile")

    mdm = MetadataManager.new(md_config=mdm_config, path=subdir1)
    mdm.touch(testfile)
    MetadataManager.new(md_config=mdm_config, path=subdir2)

    proc = subprocess.run(
        [*rm_cmd, testfile, "--repository-path", subdir2], capture_output=True
    )
    assert proc.returncode == md_constants.PATH_NOT_WITHIN_REPOSITORY
    assert not proc.stdout
    assert "fatal:" in proc.stderr.decode().lower()
    assert "traceback" not in proc.stderr.decode().lower()


@pytest.mark.a73a1db6ff
@pytest.mark.cli
@pytest.mark.touch
@pytest.mark.repository_path
@pytest.mark.debug
@pytest.mark.sanity
def test_touch_fails_if_provided_filepath_is_not_within_repositor_path_with_debug(
    working_dir, mdm_config, rm_cmd
):
    """
    Testing scenarion when --repository-path is provided but the target filepath
    that we are trying to remove is not within repository where the --repository-path
    points to.

    With --debug option.
    """

    subdir1 = working_dir.joinpath("dir1")
    subdir2 = working_dir.joinpath("dir2")
    subdir1.mkdir()
    subdir2.mkdir()

    testfile = subdir1.joinpath("testfile")

    mdm = MetadataManager.new(md_config=mdm_config, path=subdir1)
    mdm.touch(testfile)
    MetadataManager.new(md_config=mdm_config, path=subdir2)

    proc = subprocess.run(
        [*rm_cmd, testfile, "--repository-path", subdir2, "--debug"],
        capture_output=True,
    )
    assert proc.returncode == md_constants.PATH_NOT_WITHIN_REPOSITORY
    assert not proc.stdout
    assert "fatal:" in proc.stderr.decode().lower()
    assert "traceback" in proc.stderr.decode().lower()
