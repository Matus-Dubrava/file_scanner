from typing import List, Union, Optional, Tuple, Dict, Any
import shutil
from collections import Counter
import hashlib
import subprocess
from datetime import datetime
from pathlib import Path
import uuid
import sys

from sqlalchemy import or_, text

import md_constants
from md_models import FileStat, LineChanges, Config, FileORM, HistoryORM
from md_enums import FileStatus


def get_file_created_timestamp(filepath: Path) -> Union[datetime, Exception]:
    # Running "os.stat(filepath).st_ctime" doesn't return date of file creation,
    # instead it returns last date of file's metadata modification. There
    # doesn't seem to be more straightforward way to pull the file's creation
    # date on linux then this.

    # from docs (https://docs.python.org/3/library/stat.html):
    # stat.ST_CTIME
    #     The “ctime” as reported by the operating system. On some systems (like Unix)
    #     is the time of the last metadata change, and, on others (like Windows),
    #     is the creation time (see platform documentation for details).

    proc = subprocess.run(["stat", "-c", "%W", filepath], capture_output=True)

    if proc.stderr:
        return Exception(f"failed to get 'date_created' for file: {filepath}")

    timestamp = int(proc.stdout.strip())
    return datetime.fromtimestamp(timestamp)


def get_line_hash(line: str) -> str:
    line_hash = hashlib.sha256()
    line_hash.update(line.encode("utf-8"))
    return line_hash.hexdigest()


def compute_file_stats(filepath: Path) -> Union[FileStat, Exception]:
    file_hash = hashlib.sha256()
    n_lines = 0
    line_hashes: List[str] = []

    try:
        with open(filepath, "r") as f:
            for line in f:
                n_lines += 1
                file_hash.update(line.encode("utf-8"))
                line_hashes.append(get_line_hash(line))
    except Exception as err:
        return err

    return FileStat(
        n_lines=n_lines, hashes=line_hashes, file_hash=file_hash.hexdigest()
    )


def get_current_git_branch() -> Optional[str]:
    """
    Get currently checked out git branch.
    Returns None if git repository is not found.
    """
    cmd = ["git", "branch", "--show-current"]
    proc = subprocess.run(cmd, capture_output=True)
    if proc.returncode == 0:
        return None
    else:
        return proc.stdout.decode("utf-8").strip()


def get_filepath_with_delete_prefix(filepath: str | Path) -> Tuple[str, str]:
    """
    Generates unique filename/filepath for the provided filepath.

    Format: <md_delete_contant>_<uuid4>_<current_timestamp>__<original_filename>

    Returns:
    Tuple (updated_filename, updated_filepath)
    """
    uid = uuid.uuid4()
    filepath = Path(filepath)
    filename = filepath.name

    updated_filename = f"{md_constants.DELETED_PREFIX}_{uid}_{datetime.timestamp(datetime.now())}__{filename}"
    updated_filepath = f"{filepath.parent}/{updated_filename}"
    return updated_filename, updated_filepath


def count_line_changes(old_hashes: List[str], new_hashes: List[str]) -> LineChanges:
    """
    Compare hashes and get count of new lines.

    old_hashes:     list of existing hashes
    new_hashes:     list of new hashes
    """
    line_changes = LineChanges.new()
    old_c = Counter(old_hashes)
    new_c = Counter(new_hashes)

    for hash, count in new_c.items():
        if hash in old_c:
            diff = count - old_c[hash]
            line_changes.lines_added += diff if diff > 0 else 0
        else:
            line_changes.lines_added += count

    for hash, count in old_c.items():
        if hash in new_c:
            diff = count - new_c[hash]
            line_changes.lines_removed += diff if diff > 0 else 0
        else:
            line_changes.lines_removed += count

    return line_changes


def is_fs_root_dir(dir: Path, root_dir: Path = Path("/")) -> bool:
    return str(dir) == str(root_dir)


def get_mdm_root(path: Path, config: Config) -> Optional[Path]:
    """
    Returns path to Mdm repository root directory of None if root is not found
    in this or any parent directories.

    path:    Directory where to start the search
    """
    current_dir = path

    while not is_fs_root_dir(current_dir):
        if current_dir.joinpath(config.md_dir_name).exists():
            return current_dir

        current_dir = current_dir.parent

    return None


def get_mdm_root_or_exit(path: Path, config: Config) -> Path:
    """
    Returns path to Mdm repository root directory.
    Exits with code 100 if Mdm root is not found.

    path:    Directory where to start the search
    """
    maybe_mdm_root = get_mdm_root(path=path, config=config)
    if not maybe_mdm_root:
        print(
            "Not an Mdm repository (or any of the parent directories). Abort.",
            file=sys.stderr,
        )
        sys.exit(100)

    return maybe_mdm_root


def is_file_within_repository(
    repository_root: Path, filepath: Path
) -> bool | Exception:
    """
    Determine if file is within repository's root directory or any other subdirectory.

    respository_root:   Root directory of the Mdm repository.
    filepath:           Filepath to check agains.
    """
    try:
        assert (
            repository_root.is_absolute()
        ), f"Expected absolute path, got {repository_root}"
        assert filepath.is_absolute(), f"Expected absolute path, got {filepath}"
        filepath.relative_to(repository_root)
        return True
    except ValueError:
        return False
    except Exception as exc:
        return exc


def get_files_belonging_to_target_repository(
    parent_mdm, child_mdm, status_filters: Optional[List[FileStatus]] = None  # type: ignore  # noqa: F821
) -> List[Path]:
    """
    Return list of all files that are recorded in parent Mdm and
    are within child Mdm's subdirectory.

    parent_mdm:     Parent MetadataManager object.
    child_mdm:      Child MetadataManager object.
    status_filter:  List of file statuses. If provided, these will be combined into single
                    filter using "OR" operator.
    """
    query = parent_mdm.session.query(FileORM)

    if status_filters:
        combined_condition = or_(
            *[FileORM.status == status for status in status_filters]
        )
        query = query.filter(combined_condition)

    parent_file_records = query.all()

    filepaths: List[Path] = []

    for record in parent_file_records:
        if is_file_within_repository(
            repository_root=child_mdm.repository_root, filepath=record.filepath
        ):
            filepaths.append(record.filepath)

    return filepaths


def move_hash_files(source_mdm, dest_mdm, filepaths: List[Path]) -> Optional[Exception]:
    """
    Move hash files corresponding to specified files from source Mdm to destination Mdm.

    source_mdm:     Source MetadataManager object.
    dest_mdm:       Destination MetadataManager object.
    filepaths:      List of files to be synchronized between srouce and destination Mdms. All provided files must
                    be located within destination Mdm's subdirectory.
    """
    assert all(
        [
            is_file_within_repository(
                repository_root=dest_mdm.repository_root, filepath=filepath
            )
            for filepath in filepaths
        ]
    ), "Expected all files to be withing child's subdirectory structure."

    try:
        source_hash_filepaths = [
            source_mdm.get_path_to_hash_file(filepath=filepath)
            for filepath in filepaths
        ]
        dest_hash_filepaths = [
            dest_mdm.get_path_to_hash_file(filepath=filepath) for filepath in filepaths
        ]

        for source_file, dest_file in zip(source_hash_filepaths, dest_hash_filepaths):
            dest_file.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source_file, dest_file)
            source_file.unlink()
    except Exception as exc:
        return exc

    return None


def _select_and_filter_records_by_filepath(
    mdm, tablename: str, filepaths: List[Path]
) -> List[Dict[str, Any]]:
    assert filepaths, "Expected non-empty list of filepaths."

    where_condition = " OR ".join(
        [f"filepath = '{filepath}'" for filepath in filepaths]
    )
    sql = text(f"SELECT * FROM {tablename} WHERE {where_condition}")
    result = mdm.session.execute(sql)
    column_names = result.keys()
    return [
        {column_name: value for column_name, value in zip(column_names, row)}
        for row in result.fetchall()
    ]


def _insert_records(mdm, tablename: str, records: List[Dict[str, Any]]):
    assert records, "Expected records to have at least one entry."
    placeholder_str = ",".join([f":{column_name}" for column_name in records[0].keys()])
    sql = text(f"INSERT INTO {tablename} VALUES ({placeholder_str})")
    mdm.session.execute(sql, records)


def move_mdm_records(
    source_mdm, dest_mdm, filepaths: List[Path]
) -> Optional[Exception]:
    """
    Moves specified files from source Mdm to target Mdm.
    * File record from source are copied over to destination and status of file in source is set to 'TRACKED_IN_SUBREPOSITORY'
    * History records are moved over to target Mdm.
    * Custom Metadata is moved over to target Mdm.

    source_mdm:     Source MetadataManager object.
    dest_mdm:       Target MetadataManager object.
    filepaths:      List of files to be synchronized between source and destination Mdms. All provided files must
                    be located within destination Mdm's subdirectory.
    """

    # Do nothing if there are no filepaths.
    if not filepaths:
        return None

    assert all(
        [
            is_file_within_repository(
                repository_root=dest_mdm.repository_root, filepath=filepath
            )
            for filepath in filepaths
        ]
    ), "Expected all files to be withing child's subdirectory structure."

    try:
        file_records = _select_and_filter_records_by_filepath(
            mdm=source_mdm, tablename="file", filepaths=filepaths
        )
        history_records = _select_and_filter_records_by_filepath(
            mdm=source_mdm, tablename="history", filepaths=filepaths
        )
        # TODO: handle file metadata as well once implemented
        _insert_records(mdm=dest_mdm, tablename="file", records=file_records)
        _insert_records(mdm=dest_mdm, tablename="history", records=history_records)

        source_mdm.session.query(FileORM).filter(
            or_(*[FileORM.filepath == filepath for filepath in filepaths])
        ).update({FileORM.status: FileStatus.TRACKED_IN_SUBREPOSITORY})

        source_mdm.session.query(HistoryORM).filter(
            or_(*[HistoryORM.filepath == filepath for filepath in filepaths])
        ).delete()

        # TODO: This can potentially cause issues since right now there is no guarantee that
        # both of these commits will be successful and if parent's session fails during commit,
        # child's changes won't be rolled back since they were already commited.
        # This will require something like 2 Phase commit to ensure that both transactions are either
        # commited or rolled back.
        source_mdm.session.commit()
    except Exception as exc:
        source_mdm.session.rollback()
        return exc

    return None


def move_mdm_data(source_mdm, dest_mdm) -> Optional[Exception]:
    """
    Move Mdm records and hash files from source to destination Mdm. Only files that
    are within destination Mdm's subdirectory are moved.

    * File record from source are copied over to destination and status of file in source is set to 'TRACKED_IN_SUBREPOSITORY'
    * History records are moved over to target Mdm.
    * Custom Metadata is moved over to target Mdm.

    source_mdm:     Source MetadataManager object.
    dest_mdm:       Target MetadataManager object.
    """

    filepaths = get_files_belonging_to_target_repository(
        parent_mdm=source_mdm, child_mdm=dest_mdm
    )

    maybe_err = move_hash_files(
        source_mdm=source_mdm, dest_mdm=dest_mdm, filepaths=filepaths
    )
    if maybe_err:
        return maybe_err

    # TODO: add better error handling here, if moving records between Mdms fail
    # we should be able to "rollback" the changes. Easy solution would be
    # to keep hash files in both source and target mdm and delete them from
    # the source only after we are sure that all database records were moved
    # successfully.
    maybe_err = move_mdm_records(
        source_mdm=source_mdm, dest_mdm=dest_mdm, filepaths=filepaths
    )
    if maybe_err:
        return maybe_err

    return None
