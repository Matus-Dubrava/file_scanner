import sys
from pathlib import Path
from typing import Optional
import shutil

from db import create_db
from md_models import Config
from messages import Messages


class MetadataManager:
    def __init__(self, md_config: Config):
        self.md_config = md_config

    def is_fs_root_dir(self, dir: Path, root_dir: Path = Path("/")) -> bool:
        return str(dir) == str(root_dir)

    def create_md_dirs(self, where: Path):
        (where / self.md_config.md_dir_name).mkdir()
        (where / self.md_config.md_dir_name / "deleted").mkdir()
        (where / self.md_config.md_dir_name / "hashes").mkdir()

    def cleanup(self, dir: Path):
        md_dir = dir / self.md_config.md_dir_name
        if md_dir.exists():
            shutil.rmtree(md_dir)

    def check_dir_is_md_managed(
        self, dir: Path, stop_at: Optional[Path] = None
    ) -> bool:
        current_dir = dir

        while not self.is_fs_root_dir(current_dir):
            if (current_dir / self.md_config.md_dir_name).exists():
                return True

            if current_dir == stop_at:
                break

            current_dir = current_dir.parent

        return False

    def check_dir_is_git_managed(
        self, dir: Path, stop_at: Optional[Path] = None
    ) -> bool:
        current_dir = dir

        while not self.is_fs_root_dir(current_dir):
            if (current_dir / ".git").exists():
                return True

            if current_dir == stop_at:
                break

            current_dir = current_dir.parent

        return False

    def initalize_md_repository(self, dir: Path, force: bool = False) -> None:
        """
        Initliaze MD repository and sqlite database.
        Performs all neccessary check - checking for existence of Git and MD repositories
        on the path to the root.

        dir:    where to initilize the md repository
        force:  required to confirm the initialization if git repository is detected
                somewhere on the path to the root
        db_dir: where sqlite db will be initlized, mainly for testing purposes
                to trigger cleanup process
        """

        # If specified dir doesn't exist. Create one.
        if not dir.exists():
            dir.mkdir(parents=True)

        try:
            # Check if .md folder exists somewhere along the path to the root of the fs
            # Don't initialize nested .md.
            if self.check_dir_is_md_managed(dir):
                print(
                    "Detected .md repository at the path to root. Abort.",
                    file=sys.stderr,
                )
                sys.exit(1)

            # Check if .git folder exists somewhere along the path to the root of the fs
            # Print warning if .git is detected. Abort the execution and ask user to
            # provide -y flag to make sure that user is informed about this fact.
            if self.check_dir_is_git_managed(dir) and not force:
                print(
                    (
                        "Detected Git repository at the path to root. Abort.\n"
                        "Use md init -y to initialize .md repository."
                    ),
                    file=sys.stderr,
                )
                sys.exit(1)

            self.create_md_dirs(dir)

            # create sqlite metadata.db and initialize tracking table
            create_db(dir / self.md_config.md_dir_name, self.md_config.md_db_name)

            print(f"Intialized empty .md repository in {dir}")
        except Exception as err:
            self.cleanup(dir)
            print(err)
            print("Failed to initialize .md repository.")
            print("Abort.")
            sys.exit(1)

    def touch(self, dir: Path, filename: str) -> None:
        """
        ...
        """

        # TODO: fail if specified dir or any of the parent dirs is not an md dir
        if not self.check_dir_is_md_managed(dir=dir):
            print("Not an .md repository (or any of the parent directories). Abort.")
            sys.exit(1)

        # TODO: if the file already exists in the specified dir, refresh the md
        if (dir / filename).exists():
            pass
        # TODO: create empty file in the specified dir
        else:
            (dir / filename).touch()

        # TODO: create md record for the file

        # TODO: create empty hash file for the file
