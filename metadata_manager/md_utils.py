from typing import List, Union, Optional, Tuple, Dict, Any, Set
import shutil
from collections import Counter
import hashlib
import subprocess
from datetime import datetime
from pathlib import Path
import uuid
import sys

from sqlalchemy import or_, text
from sqlalchemy.orm import Session

import md_constants
from models.local_models import FileStat, LineChanges, Config, FileORM, HistoryORM
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


def get_repository_root(path: Path, config: Config) -> Optional[Path]:
    """
    Returns path to repository root directory of None if root is not found
    in this or any parent directories.

    path:    Directory where to start the search
    """
    current_dir = path

    while not is_fs_root_dir(current_dir):
        if current_dir.joinpath(config.local_dir_name).exists():
            return current_dir

        current_dir = current_dir.parent

    return None


def get_repository_root_or_exit(path: Path, config: Config) -> Path:
    """
    Returns path to repository root directory.
    Exits if root is not found.

    path:    Directory where to start the search
    """
    maybe_mdm_root = get_repository_root(path=path, config=config)
    if not maybe_mdm_root:
        print(
            "fatal: not an Mdm repository (or any of the parent directories)",
            file=sys.stderr,
        )
        sys.exit(md_constants.NOT_MDM_REPOSITORY)

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
    source_session: Session, target_repository_root: Path, status_filters: Optional[List[FileStatus]] = None  # type: ignore  # noqa: F821
) -> List[Path]:
    """
    Return list of all files that are recorded in parent repository and
    are within child respository's subdirectory.

    parent_mdm:     Parent MetadataManager object.
    child_mdm:      Child MetadataManager object.
    status_filter:  List of file statuses. If provided, these will be combined into single
                    filter using "OR" operator.
    """
    query = source_session.query(FileORM)

    if status_filters:
        combined_condition = or_(
            *[FileORM.status == status for status in status_filters]
        )
        query = query.filter(combined_condition)

    source_file_records = query.all()

    filepaths: List[Path] = []

    for record in source_file_records:
        if is_file_within_repository(
            repository_root=target_repository_root, filepath=Path(record.filepath)
        ):
            filepaths.append(Path(record.filepath))

    return filepaths


def move_hash_files(source_mdm, dest_mdm, filepaths: List[Path]) -> Optional[Exception]:
    """
    Move hash files corresponding to specified files from source to destination repository.

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
    session: Session, tablename: str, filepaths: List[Path]
) -> List[Dict[str, Any]]:
    assert filepaths, "Expected non-empty list of filepaths."

    where_condition = " OR ".join(
        [f"filepath = '{filepath}'" for filepath in filepaths]
    )
    sql = text(f"SELECT * FROM {tablename} WHERE {where_condition}")
    result = session.execute(sql)
    column_names = result.keys()
    return [
        {column_name: value for column_name, value in zip(column_names, row)}
        for row in result.fetchall()
    ]


def _insert_records(session: Session, tablename: str, records: List[Dict[str, Any]]):
    assert records, "Expected records to have at least one entry."
    placeholder_str = ",".join([f":{column_name}" for column_name in records[0].keys()])
    sql = text(f"INSERT INTO {tablename} VALUES ({placeholder_str})")
    session.execute(sql, records)


def move_mdm_records(
    source_session: Session,
    dest_session: Session,
    dest_repository_root: Path,
    filepaths: List[Path],
) -> Optional[Exception]:
    """
    Moves specified files from source repository's database to target repository's database.
    * File record from source are copied over to destination and status of file in source is set to 'TRACKED_IN_SUBREPOSITORY'
    * History records are moved over to target repository's database.
    * Custom Metadata is moved over to target repository's database.

    source_session:         Source repository database session.
    dest_session:           Destination repository database session.
    dest_repository_root:   Root directory of destination repository.
    filepaths:              List of files to be synchronized between source and destination repositories.
                            All provided files must be located within destination destination's subdirectory.
    """

    # Do nothing if there are no filepaths.
    if not filepaths:
        return None

    assert all(
        [
            is_file_within_repository(
                repository_root=dest_repository_root, filepath=filepath
            )
            for filepath in filepaths
        ]
    ), "Expected all files to be within destination's subdirectory structure."

    try:
        file_records = _select_and_filter_records_by_filepath(
            session=source_session, tablename="file", filepaths=filepaths
        )
        history_records = _select_and_filter_records_by_filepath(
            session=source_session, tablename="history", filepaths=filepaths
        )
        # TODO: handle file metadata as well once implemented
        _insert_records(session=dest_session, tablename="file", records=file_records)
        _insert_records(
            session=dest_session, tablename="history", records=history_records
        )

        source_session.query(FileORM).filter(
            or_(*[FileORM.filepath == filepath for filepath in filepaths])
        ).update({FileORM.status: FileStatus.TRACKED_IN_SUBREPOSITORY})

        source_session.query(HistoryORM).filter(
            or_(*[HistoryORM.filepath == filepath for filepath in filepaths])
        ).delete()

        # TODO: This can potentially cause issues since right now there is no guarantee that
        # both of these commits will be successful and if parent's session fails during commit,
        # child's changes won't be rolled back since they were already commited.
        # This will require something like 2 Phase commit to ensure that both transactions are either
        # commited or rolled back.
        source_session.commit()
    except Exception as exc:
        source_session.rollback()
        return exc

    return None


def move_mdm_data(
    source_session: Session, dest_session: Session, source_mdm, dest_mdm
) -> Optional[Exception]:
    """
    Move source records and hash files from source to destination repository. Only files that
    are within destination's subdirectory are moved.

    * File record from source are copied over to destination and status of file in source is set to 'TRACKED_IN_SUBREPOSITORY'
    * History records are moved over to target repository's database.
    * Custom Metadata is moved over to target repository's database.    * History records are moved over to target Mdm.

    source_session:     Source repository database session.
    dest_session:       Destination repository database session.
    source_mdm:         Source MetadataManager object.
    dest_mdm:           Target MetadataManager object.
    """

    filepaths = get_files_belonging_to_target_repository(
        source_session=source_session,
        target_repository_root=dest_mdm.repository_root,
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
        source_session=source_session,
        dest_session=dest_session,
        dest_repository_root=dest_mdm.repository_root,
        filepaths=filepaths,
    )
    if maybe_err:
        return maybe_err

    return None


def find_tracked_files_in_database(session: Session, path: Path) -> List[Path]:
    """
    Searches database for all 'ACTIVE' files associated with specified path. Handles
    both scenarios where path corresponds to a file as well as directory.
    """
    file_records = (
        session.query(FileORM)
        .filter_by(status=FileStatus.ACTIVE)
        .filter(FileORM.filepath.like(f"{path}%"))
        .all()
    )
    return [Path(record.filepath) for record in file_records]


def get_tracked_files_and_subdirectories(
    session: Session, path: Path
) -> Tuple[List[Path], Set[str]]:
    """
    Recursively traverse directory. Return list of all tracked files and a set of
    directories and subdirectories where there were at least one tracked file.

    Subdirectories that contain any tracked files or other subdirectories that contain
    any tracked files are marked as tracked.

    ex:

    a/b/c/file
    if 'file' is tracked then all 'a', 'b' and 'c' subdirectories are marked as tracked.

    a/b/c/
    if there is no tracked file (including scenarion when there is no file at all),
    none of these subdirectories are marked as tracked
    """

    assert path.exists() and path.is_dir(), f"Expected directory. Got {path}"

    tracked_files: List[Path] = []
    tracked_dirs: Set[str] = set()

    def _traverse(path: Path):
        is_tracked = False
        for child in path.iterdir():
            if (
                child.is_file()
                and session.query(FileORM).filter_by(filepath=child).first()
            ):
                is_tracked = True
                tracked_files.append(child)
            elif child.is_dir():
                is_tracked = _traverse(child)

            if is_tracked:
                tracked_dirs.add(str(path))
        return is_tracked

    _traverse(path)
    return tracked_files, tracked_dirs
