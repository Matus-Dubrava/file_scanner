import sys
from pathlib import Path
from typing import Optional, List, Union
import subprocess
import shutil
from datetime import datetime

from sqlalchemy.orm import Session

from db import create_db, get_session
from md_models import (
    Config,
    FileORM,
    FileStatus,
    FileStat,
    HistoryORM,
    VersionInfo,
    VersionInfoORM,
)
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

    def get_current_git_branch(self, dir: Path) -> Optional[str]:
        proc = subprocess.run(
            ["git", "branch", "--show-current"], capture_output=True, cwd=dir
        )
        if proc.returncode == 0:
            return proc.stdout.decode().strip()

        return None

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

    def read_line_hashes_from_hash_file(self, filepath: Path) -> List[str] | Exception:
        """
        Reads hashes from the corresponding hash file.

        filepath:       path to the original file, not the hash file
        """
        assert filepath.exists(), f"Expected {filepath} to exist."

        hashes_path_or_err = self.get_path_to_hash_file(filepath=filepath)
        if isinstance(hashes_path_or_err, Exception):
            return hashes_path_or_err

        hashes = []
        try:
            with open(hashes_path_or_err, "r") as f:
                for line in f:
                    hashes.append(line.strip())
        except Exception as err:
            return err

        return hashes

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

    def create_new_file_record(
        self,
        session: Session,
        filepath: Path,
        file_exists: bool,
        branch_name: Optional[str] = None,
    ) -> Optional[Exception]:
        try:
            if not file_exists:
                filepath.touch()

            file_stat = FileStat.new()

            if file_exists:
                file_stat_or_err = md_utils.compute_file_stats(filepath=filepath)
                if isinstance(file_stat_or_err, Exception):
                    return file_stat_or_err
                file_stat = file_stat_or_err

            hash_filepath_or_err = self.get_path_to_hash_file(filepath=filepath)
            if isinstance(hash_filepath_or_err, Exception):
                raise hash_filepath_or_err

            maybe_err = self.write_line_hashes_to_hash_file(
                filepath=filepath, line_hashes=file_stat.hashes
            )
            if maybe_err:
                raise maybe_err

            file_record = FileORM(
                filepath=str(filepath),
                version_control_branch=branch_name,
                filename=filepath.name,
                status=FileStatus.ACTIVE,
            )

            history_record = HistoryORM(
                filepath=str(filepath),
                version_control_branch=branch_name,
                fs_size=filepath.lstat().st_size if file_exists else 0,
                fs_inode=filepath.lstat().st_ino,
                count_total_lines=file_stat.n_lines,
                count_added_lines=file_stat.n_lines,
                count_removed_lines=0,
                running_added_lines=file_stat.n_lines,
                running_removed_lines=0,
                file_hash=file_stat.file_hash,
                fs_date_modified=(
                    datetime.fromtimestamp(filepath.lstat().st_mtime)
                    if file_exists
                    else datetime.now()
                ),
            )

            session.add(file_record)
            session.add(history_record)
            session.commit()
        except Exception as err:
            filepath.unlink()

            if isinstance(hash_filepath_or_err, Path):
                hash_filepath_or_err.unlink()

            return err

        return None

    def add_file_to_md(
        self,
        session: Session,
        filepath: Path,
        branch_name: Optional[str] = None,
    ) -> List[Optional[Exception]]:
        try:
            file_stat_or_err = md_utils.compute_file_stats(filepath=filepath)
            if isinstance(file_stat_or_err, Exception):
                return [file_stat_or_err]

            timestamp_created_or_err = md_utils.get_file_created_timestamp(
                filepath=filepath
            )
            if isinstance(timestamp_created_or_err, Exception):
                return [timestamp_created_or_err]

            # Read existing hashes.
            hashes_or_err = self.read_line_hashes_from_hash_file(filepath)
            if isinstance(hashes_or_err, Exception):
                return [hashes_or_err]

            # Write new hashes.
            maybe_err = self.write_line_hashes_to_hash_file(
                filepath=filepath, line_hashes=file_stat_or_err.hashes
            )
            if maybe_err:
                return [maybe_err]

            # Get the corresponding file record and potentially update branch.
            file_record = session.query(FileORM).filter_by(filepath=filepath).first()
            assert file_record, f"Expected file recrod for {filepath} to exist"
            file_record.version_control_branch = branch_name
            session.add(file_record)

            latest_history_record = (
                session.query(HistoryORM).order_by(HistoryORM.id.desc()).first()
            )
            assert latest_history_record, "Expected at least one history record."

            line_changes = md_utils.count_line_changes(
                old_hashes=hashes_or_err, new_hashes=file_stat_or_err.hashes
            )

            history_record = HistoryORM(
                filepath=str(filepath),
                version_control_branch=branch_name,
                fs_size=filepath.lstat().st_size,
                fs_inode=filepath.lstat().st_ino,
                count_total_lines=file_stat_or_err.n_lines,
                count_added_lines=line_changes.lines_added,
                count_removed_lines=line_changes.lines_removed,
                running_added_lines=latest_history_record.running_added_lines
                + line_changes.lines_added,
                running_removed_lines=latest_history_record.running_removed_lines
                + line_changes.lines_removed,
                file_hash=file_stat_or_err.file_hash,
                fs_date_modified=datetime.fromtimestamp(filepath.lstat().st_mtime),
            )

            session.add(history_record)
            session.commit()
        except Exception as err:
            errors: List[Optional[Exception]] = [err]
            maybe_err = self.remove_hash_file(filepath=filepath)
            if maybe_err:
                errors.append(err)
            return errors

        return []

    def write_version_info_to_db(self, session: Session) -> Optional[Exception]:
        version_info_or_err = VersionInfo.get_info()
        if isinstance(version_info_or_err, Exception):
            return version_info_or_err

        try:
            version_info_record = VersionInfoORM(
                commit_id=version_info_or_err.commit_id,
                version=version_info_or_err.version,
                build_type=version_info_or_err.build_type,
                build_date=version_info_or_err.build_date,
            )

            session.add(version_info_record)
            session.commit()
        except Exception as err:
            return err

        return None

    def initalize_md_repository(self, dir: Path) -> None:
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

            self.create_md_dirs(dir)

            # create sqlite metadata.db and initialize tracking table
            db_path = dir / self.md_config.md_dir_name
            create_db(db_path, self.md_config.md_db_name)

            # write version info to db
            session = get_session(db_path, self.md_config.md_db_name)
            maybe_err = self.write_version_info_to_db(session=session)
            if maybe_err:
                raise maybe_err

            session.close()

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
            session.query(FileORM).filter_by(filepath=str(filepath)).first()
        )

        maybe_errors: List[Optional[Exception]] = []

        # File doesn't exist it fs nor in the .md database.
        if not filepath.exists() and not old_file_record:
            maybe_err = self.create_new_file_record(
                session=session,
                filepath=filepath,
                branch_name=branch_name,
                file_exists=False,
            )
            maybe_errors.append(maybe_err)

        # file doesn't exist in md but exits in fs (i.e file wasnt created using md touch or due to branch switch)
        elif filepath.exists() and not old_file_record:
            maybe_err = self.create_new_file_record(
                session=session,
                filepath=filepath,
                branch_name=branch_name,
                file_exists=True,
            )
            maybe_errors.append(maybe_err)

        # file exists in md but not in fs (file was removed)
        elif not filepath.exists() and old_file_record:
            history_records = (
                session.query(HistoryORM)
                .filter_by(filepath=old_file_record.filepath)
                .all()
            )

            new_filename = md_utils.get_filename_with_delete_prefix(
                old_file_record.filename
            )
            new_filepath = md_utils.get_filepath_with_delete_prefix(
                old_file_record.filepath
            )

            # Update the File record.
            old_file_record.filename = new_filename
            old_file_record.filepath = new_filepath
            old_file_record.status = FileStatus.REMOVED
            old_file_record.timestamp_deleted = datetime.now()

            # Update assocaited history records.
            for history_record in history_records:
                history_record.filepath = new_filepath

            # Remove hash file if it exists.
            maybe_err = self.remove_hash_file(filepath=filepath)
            maybe_errors.append(maybe_err)

            # Create new file and new .md record.
            # Note that "create_new_file" intentionally commits the staged changes
            # so that it can perform cleanup if necessary. This also commits
            # the above staged changes.
            maybe_err = self.create_new_file_record(
                session=session,
                filepath=filepath,
                branch_name=branch_name,
                file_exists=False,
            )
            maybe_errors.append(maybe_err)

        # file exists in both md and fs.
        elif filepath.exists() and old_file_record:
            maybe_errors = self.add_file_to_md(
                session=session,
                filepath=filepath,
                branch_name=branch_name,
            )

        session.close()

        errors = [err for err in maybe_errors if err is not None]
        if len(errors):
            for err in errors:
                print(err, file=sys.stderr)
                sys.exit(3)

    def list_files(self, dir: Path, status_filter: Optional[FileStatus] = None) -> None:
        """
        TODO:
        List files:
            - by default show relative paths
                - show absolute pahts if --absolute flag is specified
            - by default list only active files
                - --all can be used to list all files
                - --untracked can be used to list untracked files
                - --removed can be used to list removed files
        """
        assert dir.is_absolute()

        maybe_md_root = self.get_md_root(dir=dir)
        if not maybe_md_root:
            print("Not an .md repository (or any of the parent directories). Abort.")
            sys.exit(1)

        db_path = maybe_md_root.joinpath(self.md_config.md_dir_name)
        session = get_session(db_dir=db_path, db_name=self.md_config.md_db_name)

        if status_filter:
            files = session.query(FileORM).filter_by(status=status_filter).all()
            for file in files:
                print(file.filepath)
        else:
            files = session.query(FileORM).all()
            for file in files:
                print(file.filepath)
