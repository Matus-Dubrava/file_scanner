import pytest

import tests.utils as utils
import md_utils


@pytest.mark.c0c0658d55
@pytest.mark.manager
@pytest.mark.sanity
def test_create_git_repository(working_dir):
    assert utils.initalize_git_repository(working_dir)
    assert (working_dir / ".git").exists()


@pytest.mark.c5dbb518a4
@pytest.mark.manager
@pytest.mark.sanity
def test_create_md_dirs(working_dir, mdm):
    assert (working_dir / mdm.md_config.local_dir_name).exists()
    assert (working_dir / mdm.md_config.local_dir_name / "hashes").exists()


@pytest.mark.f9308dd389
@pytest.mark.manager
@pytest.mark.sanity
@pytest.mark.cleanup
def test_cleanup(working_dir, mdm):
    utils.assert_md_structure_exists(mdm.md_config, working_dir)
    mdm.cleanup(working_dir)
    assert not (working_dir / mdm.md_config.local_dir_name).exists()


@pytest.mark.b8794b26cc
@pytest.mark.manager
@pytest.mark.sanity
@pytest.mark.get_md_root
def test_get_repository_root_exits_with_100_when_no_root_is_found(working_dir, mdm):
    mdm.cleanup(working_dir)

    with pytest.raises(SystemExit) as exit:
        not md_utils.get_repository_root_or_exit(path=working_dir, config=mdm.md_config)

    assert exit.value.code == 100


@pytest.mark.c412ccfb42
@pytest.mark.manager
@pytest.mark.sanity
@pytest.mark.get_md_root
def test_get_md_root_finds_md_repository_in_the_same_directory(working_dir, mdm):
    mdm_root = md_utils.get_repository_root_or_exit(
        path=working_dir, config=mdm.md_config
    )
    assert str(mdm_root) == str(working_dir)


@pytest.mark.ce52d3f1a2
@pytest.mark.manager
@pytest.mark.sanity
@pytest.mark.get_md_root
def test_get_md_root_finds_md_repository_in_parent_dir(working_dir, mdm):
    subdir = working_dir.joinpath("dir1", "dir2")
    subdir.mkdir(parents=True)
    maybe_md_root = md_utils.get_repository_root_or_exit(
        path=working_dir, config=mdm.md_config
    )
    assert maybe_md_root == working_dir


@pytest.mark.d954542991
@pytest.mark.manager
@pytest.mark.sanity
@pytest.mark.get_path_to_hash_file
@pytest.mark.parametrize(
    "rel_filepath", ["testfile", "dir1/testfile", "dir1/dir2/testfile"]
)
def test_get_path_to_hash_file(working_dir, mdm, rel_filepath, session):
    filepath = working_dir.joinpath(rel_filepath)
    filepath.parent.mkdir(parents=True, exist_ok=True)
    mdm.touch(session=session, filepath=filepath)

    expected_hash_filepath = working_dir.joinpath(mdm.md_path, "hashes", rel_filepath)
    hashes_path_or_err = mdm.get_path_to_hash_file(filepath)
    assert hashes_path_or_err == expected_hash_filepath


@pytest.mark.aa367e2172
@pytest.mark.manager
@pytest.mark.sanity
@pytest.mark.remove_hash_file
@pytest.mark.parametrize(
    "rel_filepath", ["testfile", "dir1/testfile", "dir1/dir2/testfile"]
)
def test_remove_hash_file_removes_the_hash_file(
    working_dir, mdm, rel_filepath, session
):
    filepath = working_dir.joinpath(rel_filepath)
    filepath.parent.mkdir(parents=True, exist_ok=True)
    mdm.touch(session=session, filepath=filepath)

    hashes_path_or_err = mdm.get_path_to_hash_file(filepath)

    maybe_err = mdm.remove_hash_file_or_dir(filepath)
    assert maybe_err is None
    assert not hashes_path_or_err.exists()
    # parent directory is expected to be kept
    assert hashes_path_or_err.parent.exists()


@pytest.mark.b947d7bb29
@pytest.mark.manager
@pytest.mark.sanity
@pytest.mark.remove_hash_file
def test_remove_hash_file_handles_missing_hash_file(working_dir, mdm, session):
    filepath = working_dir.joinpath("testfile")
    mdm.touch(session=session, filepath=filepath)

    hashes_path_or_err = mdm.get_path_to_hash_file(filepath)
    hashes_path_or_err.unlink()
    assert not hashes_path_or_err.exists()

    maybe_err = mdm.remove_hash_file_or_dir(filepath)
    assert maybe_err is None


@pytest.mark.e000bbf3ac
@pytest.mark.manager
@pytest.mark.sanity
@pytest.mark.write_line_hashes
@pytest.mark.parametrize(
    "rel_filepath", ["testfile", "dir1/testfile", "dir1/dir2/testfile"]
)
def test_write_line_hashes_to_hash_file(working_dir, mdm, rel_filepath, session):
    expected_hashes = [
        "634b027b1b69e1242d40d53e312b3b4ac7710f55be81f289b549446ef6778bee"
        "7d6fd7774f0d87624da6dcf16d0d3d104c3191e771fbe2f39c86aed4b2bf1a0f"
        "ab03c34f1ece08211fe2a8039fd6424199b3f5d7b55ff13b1134b364776c45c5"
        "63d6ff853569a0aadec5f247bba51786bb73494d1a06bdc036ebac5034a2920b"
    ]

    filepath = working_dir.joinpath(rel_filepath)
    filepath.parent.mkdir(parents=True, exist_ok=True)
    mdm.touch(session=session, filepath=filepath)

    # It is expected that new hash file is created if one doesn't exist yet.
    mdm.write_line_hashes_to_hash_file(filepath, expected_hashes[:2])
    hashes_path_or_err = mdm.get_path_to_hash_file(filepath)

    with open(hashes_path_or_err, "r") as f:
        lines = f.readlines()
        assert expected_hashes[:2] == [line.strip() for line in lines]

    # It is expected that contents of existing hash file are overriden.
    mdm.write_line_hashes_to_hash_file(filepath, expected_hashes[2:])

    with open(hashes_path_or_err, "r") as f:
        lines = f.readlines()
        assert expected_hashes[2:] == [line.strip() for line in lines]


@pytest.mark.ad06bb42e7
@pytest.mark.manager
@pytest.mark.sanity
def test_compute_repository_statistics(working_dir, mdm, session):
    file1 = working_dir.joinpath("file1")
    file2 = working_dir.joinpath("file2")
    file3 = working_dir.joinpath("file3")
    mdm.touch(session=session, filepath=file1)
    mdm.touch(session=session, filepath=file2)
    mdm.touch(session=session, filepath=file3)

    file3.write_text("\n".join(["1", "2", "3"]))
    mdm.remove_file(session=session, filepath=file3)

    file1.write_text("\n".join(["1", "2", "3"]))
    mdm.touch(session=session, filepath=file1)

    # This will overwrite all lines but line '3' will not be counted neither
    # as added or removed at this point since its 'content' technically hasn't changed.
    file1.write_text("3")
    mdm.touch(session=session, filepath=file1)

    repository_stats = mdm.compute_repository_statistics(session=session)
    assert repository_stats.active_files_count == 2
    assert repository_stats.removed_files_count == 1
    assert repository_stats.added_lines_count == 3
    assert repository_stats.removed_lines_count == 2
    assert repository_stats.total_lines_count == 1

    file_4 = working_dir.joinpath("file4")
    file_4.touch()
    file_4.write_text("1")

    mdm.add_file(session=session, filepath=file_4)

    repository_stats = mdm.compute_repository_statistics(session=session)
    assert repository_stats.active_files_count == 3
    assert repository_stats.removed_files_count == 1
    assert repository_stats.added_lines_count == 4
    assert repository_stats.removed_lines_count == 2
    assert repository_stats.total_lines_count == 2


@pytest.mark.fe67137add
@pytest.mark.manager
@pytest.mark.sanity
def test_compute_repository_statistics_works_with_empty_repository(mdm, session):

    repository_stats = mdm.compute_repository_statistics(session=session)

    assert repository_stats.active_files_count == 0
    assert repository_stats.removed_files_count == 0
    assert repository_stats.added_lines_count == 0
    assert repository_stats.removed_lines_count == 0
    assert repository_stats.total_lines_count == 0
