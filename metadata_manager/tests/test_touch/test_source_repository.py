import pytest
import subprocess
from pathlib import Path

from manager import MetadataManager
import md_constants
from md_models import FileORM


@pytest.mark.f5b0fba262
@pytest.mark.cli
@pytest.mark.touch
@pytest.mark.source_repository
@pytest.mark.sanity
def test_touch_works_when_cwd_matches_target_repository(
    working_dir, mdm_config, monkeypatch, touch_cmd
):
    subrepo1_path = working_dir.joinpath("dir1")
    subrepo1_path.mkdir()

    testfile1 = working_dir.joinpath("testfile")
    testfile2 = subrepo1_path.joinpath("testfile")

    MetadataManager.new(md_config=mdm_config, path=working_dir)
    MetadataManager.new(md_config=mdm_config, path=subrepo1_path)

    def _test_task(cwd: Path, filepath: Path) -> None:
        monkeypatch.chdir(cwd)
        subprocess.check_output([*touch_cmd, filepath])

    _test_task(cwd=working_dir, filepath=testfile1)
    _test_task(cwd=subrepo1_path, filepath=testfile2)
    _test_task(cwd=working_dir, filepath=subrepo1_path.joinpath("..", "testfile"))


@pytest.mark.f286cc57d6
@pytest.mark.cli
@pytest.mark.touch
@pytest.mark.source_repository
@pytest.mark.sanity
def test_touch_is_blocked_when_cwd_doesnt_match_target_repository(
    working_dir, mdm_config, monkeypatch, touch_cmd
):
    subrepo1_path = working_dir.joinpath("dir1")
    subrepo2_path = working_dir.joinpath("dir1", "dir2")
    subrepo1_path.mkdir()
    subrepo2_path.mkdir()

    testfile1 = subrepo1_path.joinpath("testfile")
    testfile2 = subrepo2_path.joinpath("testfile")

    MetadataManager.new(md_config=mdm_config, path=working_dir)
    MetadataManager.new(md_config=mdm_config, path=subrepo1_path)
    MetadataManager.new(md_config=mdm_config, path=subrepo2_path)

    def _test_task(cwd: Path, filepath: Path) -> None:
        with pytest.raises(subprocess.CalledProcessError) as err:
            # NOTE: Can't parallelize this as monkeypatching cwd willlead to race condition.
            monkeypatch.chdir(cwd)
            subprocess.check_output([*touch_cmd, filepath])

        assert err.value.returncode == md_constants.AMBIGUOUS_REPOSITORY
        assert not testfile1.exists()

    _test_task(cwd=working_dir, filepath=testfile1)
    _test_task(cwd=working_dir, filepath=testfile2)
    _test_task(cwd=subrepo1_path, filepath=testfile2)
    _test_task(cwd=subrepo2_path, filepath=subrepo2_path.joinpath("..", "testfile"))
    _test_task(
        cwd=subrepo2_path, filepath=subrepo2_path.joinpath("..", "..", "testfile")
    )
    _test_task(cwd=subrepo1_path, filepath=subrepo1_path.joinpath("..", "testfile"))


@pytest.mark.cf84e357a9
@pytest.mark.cli
@pytest.mark.touch
@pytest.mark.source_repository
@pytest.mark.sanity
def test_source_repository_option_overrides_cwd_and_unblocks_touch(
    working_dir, mdm_config, touch_cmd
):
    subrepo1_path = working_dir.joinpath("dir1")
    subrepo2_path = working_dir.joinpath("dir1", "dir2")
    subrepo1_path.mkdir()
    subrepo2_path.mkdir()

    testfile1 = subrepo1_path.joinpath("testfile")
    testfile2 = subrepo2_path.joinpath("testfile")

    working_dir_mdm = MetadataManager.new(md_config=mdm_config, path=working_dir)
    subrepo1_mdm = MetadataManager.new(md_config=mdm_config, path=subrepo1_path)
    subrepo2_mdm = MetadataManager.new(md_config=mdm_config, path=subrepo2_path)

    def _test_task(filepath: Path, repository_path: Path, mdm: MetadataManager) -> None:
        proc = subprocess.run(
            [*touch_cmd, filepath, "--repository-path", repository_path]
        )
        assert proc.returncode == 0
        assert filepath.exists()
        assert mdm.session.query(FileORM).filter_by(filepath=filepath.resolve()).first()
        filepath.unlink()
        mdm.remove_file(filepath=filepath)

    _test_task(filepath=testfile1, repository_path=working_dir, mdm=working_dir_mdm)
    _test_task(filepath=testfile1, repository_path=subrepo1_path, mdm=subrepo1_mdm)

    _test_task(filepath=testfile2, repository_path=working_dir, mdm=working_dir_mdm)
    _test_task(filepath=testfile2, repository_path=subrepo1_path, mdm=subrepo1_mdm)
    _test_task(filepath=testfile2, repository_path=subrepo2_path, mdm=subrepo2_mdm)

    _test_task(
        filepath=subrepo2_path.joinpath("..", "testfile"),
        repository_path=subrepo1_path,
        mdm=subrepo1_mdm,
    )
    _test_task(
        filepath=subrepo2_path.joinpath("..", "..", "testfile"),
        repository_path=working_dir,
        mdm=working_dir_mdm,
    )


@pytest.mark.baac804ddf
@pytest.mark.cli
@pytest.mark.touch
@pytest.mark.source_repository
@pytest.mark.sanity
def test_touch_fails_when_source_prepository_is_not_mdm_repository(
    working_dir, mdm_config, touch_cmd
):
    subdir = working_dir.joinpath("dir1")
    subdir.mkdir()
    filepath = working_dir.joinpath("testfile")

    MetadataManager.new(md_config=mdm_config, path=subdir)

    with pytest.raises(subprocess.CalledProcessError) as err:
        subprocess.check_output(
            [*touch_cmd, filepath, "--repository-path", working_dir]
        )

    assert err.value.returncode == md_constants.NOT_MDM_REPOSITORY


@pytest.mark.af53a8b744
@pytest.mark.cli
@pytest.mark.touch
@pytest.mark.source_repository
@pytest.mark.sanity
def test_touch_works_outside_of_mdm_repository_when_repository_path_is_provided(
    working_dir, mdm_config, touch_cmd
):
    subdir = working_dir.joinpath("dir1")
    subdir.mkdir()
    filepath = subdir.joinpath("testfile")

    MetadataManager.new(md_config=mdm_config, path=subdir)
    subprocess.check_output([*touch_cmd, filepath, "--repository-path", subdir])
