from typing import List, Union
from collections import Counter
from typing import Optional, Tuple
import hashlib
import subprocess
from datetime import datetime
from pathlib import Path
import uuid
import sys

import md_constants
from md_models import FileStat, LineChanges, Config


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
