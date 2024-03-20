import sys
from pathlib import Path
from typing import Optional, List, Union
import subprocess
import shutil
import os
from datetime import datetime

from sqlalchemy.orm import Session

from db import create_db, get_session
from md_models import Config, File, FileStatus, History
import md_utils


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

    def get_current_git_branch(self, dir: Path) -> str:
        branch_name = ""

        if self.check_dir_is_git_managed(dir):
            cwd = Path.cwd()
            os.chdir(dir)
            proc = subprocess.run(
                ["git", "branch", "--show-current"], capture_output=True
            )
            branch_name = proc.stdout.decode("utf-8").strip()
            os.chdir(cwd)

        return branch_name

    def get_md_root(self, dir: Path) -> Optional[Path]:
        """
        Retruns path to directory where .md is located or None if .md is not found
        in this or any parent directories.

        dir:    directory where to start the search
        """
        current_dir = dir

        while not self.is_fs_root_dir(current_dir):
            if (current_dir / self.md_config.md_dir_name).exists():
                return current_dir

            current_dir = current_dir.parent

        return None

    def get_path_to_hash_file(self, filepath: Path) -> Union[Exception, Path]:
        """
        Returns corresponding hash file path.

        ex: /md_dir/dir1/dir2/somefile
            ->
            /md_dir/.md/hashes/dir1/dir2/somefile
        where md_dir is where .md dir is located

        Returns error if .md repository is not found.
        """
        maybe_md_root = self.get_md_root(filepath.parent)
        if not maybe_md_root:
            return Exception("Expected .md to exist.")

        try:
            path_diff = filepath.relative_to(maybe_md_root)
            return maybe_md_root / self.md_config.md_dir_name / "hashes" / path_diff
        except Exception as err:
            # This should be unreachable because we are checking
            # for the presence of .md repository before computing relative path.
            # Added it here just to be sure.
            return err

    def remove_hash_file(self, filepath: Path) -> Optional[Exception]:
        """
        Removes hash file from .md repository. Expect .md repository have
        been initilized. Path to hash file is computed.

        filepath:   path to the original file, not the hash file
        """
        hashes_path_or_err = self.get_path_to_hash_file(filepath=filepath)
        if isinstance(hashes_path_or_err, Exception):
            return hashes_path_or_err

        filepath.unlink(missing_ok=True)
        return None

    def write_line_hashes_to_hash_file(
        self, filepath: Path, line_hashes: List[str]
    ) -> Optional[Exception]:
        """
        Creates hash file and stores line hashes there. Path to
        hash file is computed.

        filepath:       path to the original file, not the hash file
        line_hashes:    list of hashes to be written to disk
        """
        try:
            hashes_path_or_err = self.get_path_to_hash_file(filepath=filepath)
            if isinstance(hashes_path_or_err, Exception):
                return hashes_path_or_err

            hashes_path_or_err.parent.mkdir(parents=True, exist_ok=True)
            with open(hashes_path_or_err, "w") as f:
                for line_hash in line_hashes:
                    f.write(f"{line_hash}\n")

        except Exception as err:
            return err

        return None

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

    def create_new_file(
        self, session: Session, filepath: Path, branch_name: str
    ) -> Optional[Exception]:
        try:
            filepath.touch()

            file_record = File(
                filepath__git_branch=f"{filepath}__{branch_name}",
                filepath=str(filepath),
                git_branch=branch_name,
                filename=filepath.name,
                status=FileStatus.ACTIVE,
            )

            history_record = History(
                filepath__git_branch=f"{filepath}__{branch_name}",
                filepath=str(filepath),
                git_branch=branch_name,
                fs_size=0,
                fs_inode=filepath.lstat().st_ino,
                n_total_lines=0,
                n_changed_lines=0,
                running_changed_lines=0,
                file_hash="",
            )

            session.add(file_record)
            session.add(history_record)
            session.commit()
            session.close()
        except Exception as err:
            filepath.unlink()
            return err

        return None

    def add_file_to_md(
        self, session: Session, filepath: Path, branch_name: str
    ) -> Optional[List[Exception]]:
        try:
            file_stat_or_err = md_utils.compute_file_stats(filepath=filepath)
            if isinstance(file_stat_or_err, Exception):
                return [file_stat_or_err]

            timestamp_created_or_err = md_utils.get_file_created_timestamp(
                filepath=filepath
            )
            if isinstance(timestamp_created_or_err, Exception):
                return [timestamp_created_or_err]

            # Create the file containing line hashes.
            maybe_err = self.write_line_hashes_to_hash_file(
                filepath=filepath, line_hashes=file_stat_or_err.hashes
            )
            if maybe_err:
                return [maybe_err]

            file_record = File(
                filepath__git_branch=f"{filepath}__{branch_name}",
                filepath=str(filepath),
                fs_timestamp_created=timestamp_created_or_err,
                git_branch=branch_name,
                filename=filepath.name,
                status=FileStatus.ACTIVE,
            )

            history_record = History(
                filepath__git_branch=f"{filepath}__{branch_name}",
                filepath=str(filepath),
                git_branch=branch_name,
                fs_size=filepath.lstat().st_size,
                fs_inode=filepath.lstat().st_ino,
                n_total_lines=file_stat_or_err.n_lines,
                n_changed_lines=file_stat_or_err.n_lines,
                running_changed_lines=file_stat_or_err.n_lines,
                file_hash=file_stat_or_err.file_hash,
                fs_date_modified=datetime.fromtimestamp(filepath.lstat().st_mtime),
            )

            session.add(file_record)
            session.add(history_record)
            session.commit()
            session.close()

        except Exception as err:
            errors: List[Exception] = [err]

            # If we are here, there was a problem with database operation.
            # Need to remove the hashes file if the record was not stored in the
            # .md database.
            maybe_err = self.remove_hash_file(filepath=filepath)
            if maybe_err:
                errors.append(err)
            return errors

        return None

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

        # Relative path can lead to bugs and infinite loops when traversing fs structure.
        assert dir.is_absolute(), f"Expected aboslute path. Got {dir}."

        # If specified dir doesn't exist. Create one.
        if not dir.exists():
            dir.mkdir(parents=True)

        try:
            # Check if .md folder exists somewhere along the path to the root of the fs
            # Don't initialize nested .md.
            if self.check_dir_is_md_managed(dir):
                print(
                    ".md repository exists in this or one of the parent directories. Abort.",
                    file=sys.stderr,
                )
                sys.exit(1)

            # TODO: is this really necessary? We should be able to handle this without
            # bothering user. The only thing that can be potentially problematic here
            # is analytics built on top of md but that should probably be handled there.
            # Also we are creating artificial connection between MD and Git. Ideally
            # we could support multiple version control systems in future and then
            # this will make even less sense.
            #
            # Check if .git folder exists somewhere along the path to the root of the fs
            # Print warning if .git is detected. Abort the execution and ask user to
            # provide -y flag to make sure that user is informed about this fact.
            if self.check_dir_is_git_managed(dir) and not force:
                print(
                    (
                        "Git repository exists in this or one of the parent directories. Abort.\n"
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

    def touch(self, filepath: Path) -> None:
        """
        ...
        """
        assert filepath.is_absolute()

        if not filepath.parent.exists():
            print(f"Directory {filepath.parent} doesn't exist. Abort.")
            sys.exit(1)

        maybe_md_root = self.get_md_root(dir=filepath.parent)
        if not maybe_md_root:
            print("Not an .md repository (or any of the parent directories). Abort.")
            sys.exit(1)

        session = get_session(
            maybe_md_root / self.md_config.md_dir_name, self.md_config.md_db_name
        )
        branch_name = self.get_current_git_branch(filepath.parent)

        old_file_record = (
            session.query(File)
            .filter_by(filepath__git_branch=f"{filepath}__{branch_name}")
            .first()
        )

        # File doesn't exist it fs nor in the .md database.
        if not filepath.exists() and not old_file_record:
            maybe_err = self.create_new_file(
                session=session, filepath=filepath, branch_name=branch_name
            )
        # file doesn't exist in md but exits in fs (i.e file wasnt created using md touch or due to branch switch)
        #   - create new md record, create populated hashes file
        elif filepath.exists() and not old_file_record:
            maybe_errors = self.add_file_to_md(
                session=session, filepath=filepath, branch_name=branch_name
            )
        # file exists in md but not in fs (file was removed)
        #   - add new record to `file` table, prefix it with .delteted__
        #       - update both filepath, filename, ...
        #   - update the records in `history` to match the primary_key
        #   - create new file and new md record, create empty hashes file
        # file exists in both md and fs
        #   - add new entry to history

        # TODO: clean this up
        if maybe_err:
            raise maybe_err
        if maybe_errors:
            for err in maybe_errors:
                print(err, file=sys.stderr)
                sys.exit(3)
