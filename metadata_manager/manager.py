import os
import sys
from pathlib import Path
from typing import Optional


def is_fs_root_dir(dir: Path, root_dir: Path = Path("/")) -> bool:
    return str(dir) == str(root_dir)


def create_md_dirs(where: Path):
    (where / ".md").mkdir()
    (where / ".md" / "deleted").mkdir()
    (where / ".md" / "hashes").mkdir()


def check_dir_is_md_managed(dir: Path, stop_at: Optional[Path] = None) -> bool:
    current_dir = dir

    while not is_fs_root_dir(current_dir):
        if (current_dir / ".md").exists():
            return True

        if current_dir == stop_at:
            break

        current_dir = current_dir.parent

    return False


def check_dir_is_git_managed(dir: Path, stop_at: Optional[Path] = None) -> bool:
    current_dir = dir

    while not is_fs_root_dir(current_dir):
        if (current_dir / ".git").exists():
            return True

        if current_dir == stop_at:
            break

        current_dir = current_dir.parent

    return False


def initalize_md_repository(dir: Path, force: bool = False) -> bool:
    print("HERE")
    assert dir.exists()
    # Check if .md folder exists somewhere along the path to the root of the fs
    # Don't initialize nested .md.
    print("checking md")
    if check_dir_is_md_managed(dir):
        print("Detected MD repository at the path to root. Abort.", file=sys.stderr)
        sys.exit(1)
    print("md checked")

    # Check if .git folder exists somewhere along the path to the root of the fs
    # Print warning if .git is detected. Abort the execution and ask user to
    # provide -y flag to make sure that user is informed about this fact.
    if check_dir_is_git_managed(dir) and not force:
        print(
            (
                "Detected Git repository at the path to root. Abort.\n"
                "Use md init -y to initialize MD repository."
            ),
            file=sys.stderr,
        )
        sys.exit(1)

    create_md_dirs(dir)

    # create sqlite metadata.db and initialize tracking table
    return False
