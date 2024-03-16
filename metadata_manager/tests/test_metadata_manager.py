import pytest
import os
import shutil
import subprocess

from metadata_manager.metadata_manager import (
    load_metadata_from_file,
    is_file_tracked,
    remove_tracking_attribute,
)


def touch_file(dirname: str, filename: str) -> str:
    filepath = os.path.join(dirname, filename)
    with open(filepath, "w"):
        pass
    return filepath


def assert_no_stderr(proc: subprocess.CompletedProcess[bytes]) -> None:
    assert (
        not proc.stderr
    ), f"Expected empty stderr message. Recevided: {str(proc.stderr)}"


def assert_return_code(expected: int, received: int) -> None:
    assert (
        expected == received
    ), f"Expected return code {expected}. Received {received}."


def validate_metadata_contains_all_tracked_files(
    dirname: str, metadata_filename: str
) -> None:
    tracked_files = [
        filename
        for filename in os.listdir(dirname)
        if is_file_tracked(os.path.join(dirname, filename))
    ]

    _error_occured, metadata = load_metadata_from_file(dirname, metadata_filename)
    assert set(tracked_files) == set(metadata.tracked_files.keys())


@pytest.fixture(scope="module")
def metadata_manager_cmd():
    manager_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "metadata_manager.py",
    )
    yield "python3", manager_path


@pytest.fixture(scope="function")
def working_dir():
    working_dir_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "working_directory"
    )
    if os.path.exists(working_dir_path):
        shutil.rmtree(working_dir_path)

    os.makedirs(working_dir_path)
    yield working_dir_path
    # shutil.rmtree(working_dir_path)


@pytest.fixture(scope="function")
def metadata_filename(metadata_manager_cmd):
    proc = subprocess.run(
        [*metadata_manager_cmd, "--show-metadata-file-name"], capture_output=True
    )

    yield str(proc.stdout.strip()).removeprefix("b'").removesuffix("'")


@pytest.fixture(scope="module")
def help_message():
    yield "usage: File Metadata Manager".lower()


@pytest.fixture(scope="module")
def corrupted_metadata_message():
    yield "Error: Detected metadata corruption.".lower()


@pytest.mark.tc_1
@pytest.mark.show_metadata_file_name
@pytest.mark.options
@pytest.mark.sanity
def test_show_metadata_file_name_returns_nonempy_response(metadata_manager_cmd):
    proc = subprocess.run(
        [*metadata_manager_cmd, "--show-metadata-file-name"], capture_output=True
    )

    assert_no_stderr(proc)
    assert_return_code(0, proc.returncode)

    assert len(proc.stdout) != 0


@pytest.mark.tc_11
@pytest.mark.show_metadata_file_name
@pytest.mark.help
@pytest.mark.options
@pytest.mark.sanity
def test_show_metadta_file_name_fails_if_it_is_not_the_only_argument(
    metadata_manager_cmd, help_message
):
    proc = subprocess.run(
        [*metadata_manager_cmd, "--show-metadata-file-name", "--verbose"],
        capture_output=True,
    )

    assert_return_code(1, proc.returncode)
    assert_no_stderr(proc)
    assert help_message in str(proc.stdout).lower()


@pytest.mark.tc_2
@pytest.mark.add
@pytest.mark.options
@pytest.mark.sanity
@pytest.mark.parametrize("add_file_argument", ["--add", "-a"])
def test_add_works_with_exactly_one_file(
    working_dir, metadata_manager_cmd, metadata_filename, add_file_argument
):
    test_file = touch_file(working_dir, "test_file")

    proc = subprocess.run(
        [*metadata_manager_cmd, add_file_argument, test_file, "--verbose"],
        capture_output=True,
    )

    assert_no_stderr(proc)
    assert_return_code(0, proc.returncode)

    _error_occured, metadata = load_metadata_from_file(working_dir, metadata_filename)

    assert os.path.basename(test_file) in metadata.tracked_files
    validate_metadata_contains_all_tracked_files(working_dir, metadata_filename)


@pytest.mark.tc_3
@pytest.mark.add
@pytest.mark.options
@pytest.mark.sanity
@pytest.mark.parametrize("add_file_argument", ["--add", "-a"])
def test_add_can_be_called_multiple_times_with_different_filenames(
    working_dir, metadata_manager_cmd, metadata_filename, add_file_argument
):
    test_files = [
        touch_file(working_dir, "test_file_1"),
        touch_file(working_dir, "test_file_2"),
        touch_file(working_dir, "test_file_3"),
    ]

    for file in test_files:
        proc = subprocess.run(
            [*metadata_manager_cmd, add_file_argument, file, "--verbose"],
            capture_output=True,
        )

        assert_no_stderr(proc)
        assert_return_code(0, proc.returncode)

    _error_occured, metadata = load_metadata_from_file(working_dir, metadata_filename)

    for file in test_files:
        assert os.path.basename(file) in metadata.tracked_files

    validate_metadata_contains_all_tracked_files(working_dir, metadata_filename)


@pytest.mark.tc_4
@pytest.mark.add
@pytest.mark.options
@pytest.mark.sanity
@pytest.mark.parametrize("add_file_argument", ["--add", "-a"])
def test_add_works_with_multiple_files_provided_in_single_call(
    working_dir, metadata_manager_cmd, metadata_filename, add_file_argument
):
    test_files = [
        touch_file(working_dir, "test_file_1"),
        touch_file(working_dir, "test_file_2"),
        touch_file(working_dir, "test_file_3"),
    ]

    proc = subprocess.run(
        [*metadata_manager_cmd, add_file_argument, *test_files, "--verbose"],
        capture_output=True,
    )

    assert_no_stderr(proc)
    assert_return_code(0, proc.returncode)

    _error_occured, metadata = load_metadata_from_file(working_dir, metadata_filename)

    for file in test_files:
        assert os.path.basename(file) in metadata.tracked_files

    validate_metadata_contains_all_tracked_files(working_dir, metadata_filename)


@pytest.mark.tc_5
@pytest.mark.add
@pytest.mark.options
@pytest.mark.sanity
@pytest.mark.parametrize("add_file_argument", ["--add", "-a"])
def test_add_filters_out_duplicate_from_users_input(
    working_dir, metadata_manager_cmd, metadata_filename, add_file_argument
):
    test_files = [
        touch_file(working_dir, "test_file_1"),
        touch_file(working_dir, "test_file_2"),
        touch_file(working_dir, "test_file_2"),
    ]

    proc = subprocess.run(
        [*metadata_manager_cmd, add_file_argument, *test_files, "--verbose"],
        capture_output=True,
    )

    assert_no_stderr(proc)
    assert_return_code(0, proc.returncode)

    _error_occured, metadata = load_metadata_from_file(working_dir, metadata_filename)

    for file in test_files:
        assert os.path.basename(file) in metadata.tracked_files

    assert len(metadata.tracked_files.values()) == 2
    validate_metadata_contains_all_tracked_files(working_dir, metadata_filename)


@pytest.mark.tc_6
@pytest.mark.add
@pytest.mark.options
@pytest.mark.sanity
@pytest.mark.parametrize("add_file_argument", ["--add", "-a"])
def test_add_doesnt_store_duplicate_values_when_record_already_exists_in_file(
    working_dir, metadata_manager_cmd, metadata_filename, add_file_argument
):
    test_files = [
        touch_file(working_dir, "test_file_1"),
        touch_file(working_dir, "test_file_2"),
    ]

    proc = subprocess.run(
        [*metadata_manager_cmd, add_file_argument, *test_files, "--verbose"],
        capture_output=True,
    )

    assert_no_stderr(proc)
    assert_return_code(0, proc.returncode)

    _error_occured, metadata = load_metadata_from_file(working_dir, metadata_filename)

    for file in test_files:
        assert os.path.basename(file) in metadata.tracked_files

    assert len(metadata.tracked_files.values()) == 2

    # Add the same files again.
    proc = subprocess.run(
        [*metadata_manager_cmd, add_file_argument, *test_files, "--verbose"],
        capture_output=True,
    )

    assert_no_stderr(proc)
    assert_return_code(0, proc.returncode)

    _error_occured, metadata = load_metadata_from_file(working_dir, metadata_filename)

    for file in test_files:
        assert os.path.basename(file) in metadata.tracked_files

    assert len(metadata.tracked_files.values()) == 2
    validate_metadata_contains_all_tracked_files(working_dir, metadata_filename)


@pytest.mark.tc_7
@pytest.mark.add
@pytest.mark.options
@pytest.mark.sanity
@pytest.mark.parametrize("add_file_argument", ["--add", "-a"])
def test_add_fails_when_nonexistent_file_is_specified(
    working_dir, metadata_manager_cmd, metadata_filename, add_file_argument
):

    proc = subprocess.run(
        [
            *metadata_manager_cmd,
            add_file_argument,
            "this_file_doesnt_exist",
            "--verbose",
        ],
        capture_output=True,
    )

    assert_return_code(1, proc.returncode)

    assert not os.path.exists(os.path.join(working_dir, metadata_filename))


@pytest.mark.tc_9
@pytest.mark.add
@pytest.mark.options
@pytest.mark.sanity
@pytest.mark.parametrize("add_file_argument", ["--add", "-a"])
def test_add_automatically_recreate_corrupted_metadata(
    working_dir, metadata_manager_cmd, metadata_filename, add_file_argument
):
    test_file = touch_file(working_dir, "test_file")

    proc = subprocess.run(
        [*metadata_manager_cmd, add_file_argument, test_file, "--verbose"],
        capture_output=True,
    )

    assert_return_code(0, proc.returncode)
    assert_no_stderr(proc)

    # Corrupt metadata file.
    with open(os.path.join(working_dir, metadata_filename), "a") as f:
        f.write("#########################")

    proc = subprocess.run(
        [*metadata_manager_cmd, add_file_argument, test_file, "--verbose"],
        capture_output=True,
    )

    assert_return_code(0, proc.returncode)

    _error_occured, metadata = load_metadata_from_file(working_dir, metadata_filename)
    assert os.path.basename(test_file) in metadata.tracked_files.keys()
    validate_metadata_contains_all_tracked_files(working_dir, metadata_filename)


@pytest.mark.tc_10
@pytest.mark.options
@pytest.mark.help
@pytest.mark.sanity
def test_help_is_shown_when_no_arguments_are_provided(metadata_manager_cmd):
    expected_msg = "usage: File Metadata Manager"

    proc = subprocess.run(
        [*metadata_manager_cmd],
        capture_output=True,
    )

    assert_return_code(1, proc.returncode)
    assert_no_stderr(proc)
    assert expected_msg in str(proc.stdout)


@pytest.mark.tc_13
@pytest.mark.options
@pytest.mark.list_tracked_files
@pytest.mark.sanity
@pytest.mark.parametrize("list_tracked_files_flag", ["--list-tracked-files", "-lt"])
def test_list_tracked_files_works_when_directory_is_empty(
    metadata_manager_cmd, working_dir, list_tracked_files_flag
):
    cmd = [*metadata_manager_cmd, list_tracked_files_flag, working_dir]
    proc = subprocess.run(
        cmd,
        capture_output=True,
    )

    assert_return_code(0, proc.returncode)
    assert_no_stderr(proc)
    assert not proc.stdout


@pytest.mark.tc_14
@pytest.mark.options
@pytest.mark.list_tracked_files
@pytest.mark.sanity
@pytest.mark.parametrize("list_tracked_files_flag", ["--list-tracked-files", "-lt"])
def test_list_tracked_files_work_when_metadata_file_is_missing(
    metadata_manager_cmd, working_dir, metadata_filename, list_tracked_files_flag
):
    test_file = touch_file(working_dir, "test_file")

    cmd = [*metadata_manager_cmd, "--add", test_file]
    proc = subprocess.run(cmd, capture_output=True)
    assert_return_code(0, proc.returncode)

    # remove the metadata file
    os.remove(os.path.join(working_dir, metadata_filename))

    cmd = [*metadata_manager_cmd, list_tracked_files_flag, working_dir]
    proc = subprocess.run(cmd, capture_output=True)
    assert_return_code(0, proc.returncode)
    assert_no_stderr(proc)

    _error_occured, metadata = load_metadata_from_file(working_dir, metadata_filename)
    assert os.path.basename(test_file) in metadata.tracked_files.keys()
    validate_metadata_contains_all_tracked_files(working_dir, metadata_filename)


@pytest.mark.tc_15
@pytest.mark.options
@pytest.mark.list_tracked_files
@pytest.mark.sanity
@pytest.mark.parametrize("list_tracked_files_flag", ["--list-tracked-files", "-lt"])
def test_list_traked_files_marks_all_files_that_missing_tracking_attribute(
    metadata_manager_cmd, working_dir, metadata_filename, list_tracked_files_flag
):
    test_file = touch_file(working_dir, "test_file")

    cmd = [*metadata_manager_cmd, "--add", test_file]
    proc = subprocess.run(cmd, capture_output=True)
    assert_return_code(0, proc.returncode)

    # Remove tracking attribute
    error_occured = remove_tracking_attribute(test_file)
    assert not error_occured

    # Confirm file is tracked in metadata file.
    _error_occured, metadata = load_metadata_from_file(working_dir, metadata_filename)
    assert os.path.basename(test_file) in metadata.tracked_files.keys()

    # Confirm that --list-tracked-files marks the untracked file.
    cmd = [*metadata_manager_cmd, list_tracked_files_flag, working_dir]
    proc = subprocess.run(cmd, capture_output=True)
    assert_return_code(0, proc.returncode)

    assert is_file_tracked(test_file)
    validate_metadata_contains_all_tracked_files(working_dir, metadata_filename)


# TODO:
# test remove file logic
# test that multiple arguments cannot be at the same time
