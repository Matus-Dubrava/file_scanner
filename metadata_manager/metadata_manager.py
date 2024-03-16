import argparse
import subprocess
import sys
import os
from pathlib import Path
import json
from typing import List, Dict, Tuple
from pydantic import BaseModel, ValidationError
from datetime import datetime

METADATA_FILE_NAME = ".tracked_metadata.json"


class FileMetadata(BaseModel):
    tracked_since: str


class Metadata(BaseModel):
    # CAUTION. Changing this structure can easily break the compatibility
    # with already existing metadata files. Migrations will be painful.

    tracked_files: Dict[str, FileMetadata]
    dirname: str

    @staticmethod
    def new(dirname: str) -> "Metadata":
        return Metadata(dirname=dirname, tracked_files={})

    def add_tracking_attribute_to_all_files(self) -> None:
        for filename in self.tracked_files.keys():
            add_tracking_file_attribute(dirname=self.dirname, filename=filename)


def add_tracking_file_attribute(dirname: str, filename: str) -> None:
    """
    Adds file attribute to file's metadata.
    """
    filepath = os.path.join(dirname, filename)
    proc = subprocess.run(
        ["setfattr", "-n", "user.tracked", "-v", '"true"', filepath],
        capture_output=True,
    )
    assert proc.returncode == 0


def is_file_tracked(filepath: str) -> bool:
    proc = subprocess.run(
        ["getfattr", "-n", "user.tracked", filepath],
        capture_output=True,
    )
    return proc.returncode == 0


def remove_tracking_attribute(filepath: str) -> bool:
    """
    Returns error flag set to True if underlying command
    returned non-zero return code.
    """
    cmd = ["setfattr", "-x", "user.tracked", filepath]
    proc = subprocess.run(cmd, capture_output=True)
    error_occured = proc.returncode != 0
    return error_occured


def synchronize_tracked_files_with_metadata_file(dirname: str) -> Metadata:
    """
    List tracked files in current working directory.
    Files that are tracked have special attribute.
    Metadata file is used as a backup if the attribute is removed from
    the file by some external utility. ex: git operations can remove
    the tracking attribute when updating the file.

    This function synchronizes tracked files (based on file attribute)
    with records in metadata file in both ways. Always preserving
    records and adding either missing records or missing attributes.
    """
    metadata_filepath = os.path.join(dirname, METADATA_FILE_NAME)
    current_datetime = str(datetime.now())
    filenames = os.listdir(dirname)
    tracked_files: List[str] = []

    for filename in filenames:
        filepath = os.path.join(dirname, filename)
        if is_file_tracked(filepath):
            tracked_files.append(filename)

    # If file is tracked but it is for some reason not in the metadata
    # file or metadata file is missing, add it back to the metadata file.
    write_required = False
    error_occured, metadata = load_metadata_from_file(
        dirname=dirname, metadata_filename=METADATA_FILE_NAME
    )

    if error_occured:
        write_required = True

    for filename in tracked_files:
        if filename not in metadata.tracked_files.keys():
            # Found a missing file, therefore the write is required.
            write_required = True
            metadata.tracked_files[filename] = FileMetadata(
                tracked_since=current_datetime
            )

    if write_required:
        with open(metadata_filepath, "w") as f:
            f.write(metadata.model_dump_json())

    # Check the other way around - if every file tracked in metadata has the tracking attibute set.
    # If not, set the tracking attribute.
    for filename in metadata.tracked_files.keys():
        if filename not in tracked_files:
            tracked_files.append(filename)
            proc = subprocess.run(
                [
                    "setfattr",
                    "-n",
                    "user.tracked",
                    "-v",
                    '"true"',
                    os.path.join(dirname, filename),
                ],
                capture_output=True,
            )
            assert (
                proc.returncode == 0
            ), f"Expected process exit sucessfully. Return code: {proc.returncode}. stderr: {str(proc.stderr)}"

    return metadata


def validate_provided_filenames(filenames: List[str]) -> None:
    """
    Validate every provided file exists and all of the files
    are located within the same folder.
    """
    assert len(filenames), f"Expected non-empty list of files. Received: {filenames}."

    dirname = os.path.dirname(os.path.abspath(filenames[0]))

    for filename in filenames:
        if not os.path.exists(filename):
            error_msg = f"Error: Provided file {filename} doesn't exist."
            print(error_msg, file=sys.stderr)
            sys.exit(1)

        if os.path.dirname(os.path.abspath(filename)) != dirname:
            error_msg = (
                "Error: Invalid file path specified.\n"
                "Only files within the same folder can be specified.\n"
                "Detected multiple folders:\n"
                f"\t{dirname}\n\t{os.path.dirname(os.path.abspath(filename))}"
            )
            print(error_msg, file=sys.stderr)
            sys.exit(1)


def validate_args(parser: argparse.ArgumentParser) -> argparse.Namespace:
    args = parser.parse_args()

    # Print help and exit if no arguments were provided.
    if len(sys.argv) == 1:
        parser.print_help(file=sys.stdout)
        sys.exit(1)

    # --show-metadata-file-name must be the only argument if provided.
    if args.show_metadata_file_name and len(sys.argv) != 2:
        parser.print_help(file=sys.stdout)
        sys.exit(1)

    # --list-tracked-files must be the only argument if provided together with its value.
    if args.list_tracked_files and len(sys.argv) != 3:
        parser.print_help(file=sys.stdout)
        sys.exit(1)

    # TODO: make sure that files_to_add & files_to_remove cannot be used
    # in combination with any other argument

    args.dirname = None

    if args.files_to_add:
        validate_provided_filenames(args.files_to_add)
        args.dirname = os.path.dirname(os.path.abspath(args.files_to_add[0]))

    if args.files_to_remove:
        validate_provided_filenames(args.files_to_remove)
        args.dirname = os.path.dirname(os.path.abspath(args.files_to_remove[0]))

    return args


def load_metadata_from_file(
    dirname: str, metadata_filename: str = METADATA_FILE_NAME
) -> Tuple[bool, Metadata]:
    """
    Read metadata from file into Metadata object.
    If metadata file doesn't exists, create new one and
    return default Metadata object.
    Boolean error flag is set to True if the file is either missing
    or the metadata is corrupted.
    """
    metadata_filepath = os.path.join(dirname, metadata_filename)
    metadata = Metadata.new(dirname)
    error_occured = False

    if not os.path.exists(metadata_filepath):
        Path.touch(Path(metadata_filepath))
        error_occured = True
    else:
        try:
            with open(os.path.join(dirname, metadata_filename), "r") as f:
                metadata = Metadata.model_validate_json(f.read())
        except ValidationError:
            error_occured = True

    return error_occured, metadata


def remove_metadata_file(
    dirname: str, metadata_filename: str = METADATA_FILE_NAME
) -> None:
    metadata_filepath = os.path.join(dirname, metadata_filename)
    if os.path.exists(metadata_filepath):
        os.remove(metadata_filepath)


def add_records_to_metadata_file_and_set_tracking_attribute(
    dirname: str,
    filenames: List[str],
) -> None:
    """
    Creates metadata info for specified files and store them in the metadata file together
    with existing entries. Creates new metadata file if one doesn't exist yet.
    """
    metadata_filepath = os.path.join(dirname, METADATA_FILE_NAME)
    current_datetime = str(datetime.now())
    metadata = Metadata.new(dirname=dirname)
    new_metadata = Metadata(
        dirname=dirname,
        tracked_files={
            filename: FileMetadata(tracked_since=current_datetime)
            for filename in filenames
        },
    )

    _error_occured, metadata = load_metadata_from_file(dirname)

    # Add new entry only for files that are not yet present in the metadata.
    for filename in filenames:
        if filename not in metadata.tracked_files.keys():
            metadata.tracked_files[filename] = new_metadata.tracked_files[filename]

    metadata.add_tracking_attribute_to_all_files()
    with open(metadata_filepath, "w") as f:
        f.write(json.dumps(metadata.model_dump()))


def remove_records_from_metadata_file_and_remove_tracking_attributes(
    dirname: str, filenames: List[str]
):
    metadata_filepath = os.path.join(dirname, METADATA_FILE_NAME)
    metadata = synchronize_tracked_files_with_metadata_file(dirname)

    for filename in filenames:
        remove_tracking_attribute(os.path.join(dirname, filename))
        del metadata.tracked_files[filename]
        with open(metadata_filepath, "w") as f:
            f.write(metadata.model_dump_json())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="File Metadata Manager")
    parser.add_argument("--add", "-a", dest="files_to_add", nargs="+")
    parser.add_argument(
        "--remove", "-r", required=False, dest="files_to_remove", nargs="+"
    )
    parser.add_argument("--verbose", default=False, action="store_true")
    parser.add_argument(
        "--show-metadata-file-name",
        default=False,
        action="store_true",
        help="If provided, it must be the only flag. Returns the name of the metadata file.",
    )
    parser.add_argument(
        "--list-tracked-files",
        "-lt",
        required=False,
        nargs="?",
        const=os.getcwd(),
        default=False,
        help=("Optionally specify dirname. Defaults to current working directory."),
    )

    # Note: Additonal field "dirname" is going to be added to args during validation
    # representing the folder common for all specified files. If provided files are
    # located in multiple folders, execution wil be aborted.
    args = validate_args(parser)

    if args.list_tracked_files:
        metadata = synchronize_tracked_files_with_metadata_file(args.list_tracked_files)
        for filename in metadata.tracked_files.keys():
            print(filename)

    elif args.files_to_add:
        add_records_to_metadata_file_and_set_tracking_attribute(
            dirname=args.dirname,
            filenames=[os.path.basename(filename) for filename in args.files_to_add],
        )
    elif args.files_to_remove:
        remove_records_from_metadata_file_and_remove_tracking_attributes(
            dirname=args.dirname,
            filenames=[os.path.basename(filename) for filename in args.files_to_remove],
        )
    elif args.show_metadata_file_name:
        print(METADATA_FILE_NAME)
