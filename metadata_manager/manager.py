import sys
import traceback
from pathlib import Path
from typing import Optional, List, Union
import subprocess
import shutil
from datetime import datetime

from sqlalchemy.orm import Session

from db import create_or_get_session
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
    def __init__(
        self,
        md_config: Config,
        repository_root: Path,
        md_path: Path,
        md_db_path: Path,
        session: Session,
    ):
        self.md_config = md_config
        self.repository_root = repository_root
        self.md_path = md_path
        self.md_db_path = md_db_path
        self.session = session

    @staticmethod
    def new(md_config: Config, path: Path):
        assert path.is_absolute(), f"Expected aboslute path. Got {path}."

        # If specified directory doesn't exist. Create one.
        if not path.exists():
            path.mkdir(parents=True)

        # TODO: remove this and add support for subrepositories
        # this is currently untested
        if md_utils._get_mdm_root(path=path, config=md_config):
            print(
                "Mdm repository already exists in this or parent dir. Abort",
                file=sys.stderr,
            )
            sys.exit(200)

        # if no mdm root exits, create a new mdm repository
        # if mdm root already exits, create new mdm repository and
        # sync the new repository with the parent repo
        #   -   all files that are under the new repository and are in ACTIVE or UNTRACKED state
        #       are copied over to the new repo
        #   -   in the old repo, these files are marked as SUBREPOSITORY_TRACKED
        #       together with the filepath of the subrepository
        #       -   this needs to be a new file attribute

        md_path = path.joinpath(md_config.md_dir_name)
        md_db_path = md_path.joinpath(md_config.md_db_name)

        md_path.mkdir()
        md_path.joinpath("deleted").mkdir()
        md_path.joinpath("hashes").mkdir()

        session_or_err = create_or_get_session(md_db_path)
        if isinstance(session_or_err, Exception):
            print(f"{session_or_err}\n", file=sys.stderr)
            print("Failed to connect to Mdm database.", file=sys.stderr)
            sys.exit(101)

        mdm = MetadataManager(
            md_config=md_config,
            repository_root=path,
            md_path=md_path,
            md_db_path=md_db_path,
            session=session_or_err,
        )

        maybe_err = mdm.write_version_info_to_db()
        if maybe_err:
            mdm.cleanup(path)
            print(maybe_err)
            print("Failed to initialize .md repository.", file=sys.stderr)
            print("Abort.", file=sys.stderr)
            sys.exit(1)

        print(f"Intialized empty .md repository in {path}")
        return mdm

    @staticmethod
    def from_repository(md_config: Config, path: Path):
        assert path.is_absolute(), f"Expected aboslute path. Got {path}."

        maybe_md_root = md_utils.get_mdm_root_or_exit(path=path, config=md_config)

        md_path = maybe_md_root.joinpath(md_config.md_dir_name)
        md_db_path = md_path.joinpath(md_config.md_db_name)
        # TODO: handle this more gracefully
        assert md_path.exists()

        # if path contains .md repo, load all necessary data from there
        session_or_err = create_or_get_session(md_db_path)
        if isinstance(session_or_err, Exception):
            print(f"{session_or_err}\n", file=sys.stderr)
            print("Failed to connect to Mdm database.", file=sys.stderr)
            sys.exit(101)

        return MetadataManager(
            md_config=md_config,
            repository_root=maybe_md_root,
            md_path=md_path,
            md_db_path=md_db_path,
            session=session_or_err,
        )

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

    def get_path_to_hash_file(self, filepath: Path) -> Union[Exception, Path]:
        """
        Returns corresponding hash file path.

        ex: /md_dir/dir1/dir2/somefile
            ->
            /md_dir/.md/hashes/dir1/dir2/somefile
        where md_dir is where .md dir is located

        Returns error if .md repository is not found.
        """

        try:
            path_diff = filepath.relative_to(self.repository_root)
            return self.md_path.joinpath("hashes", path_diff)
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

        hashes_path_or_err.unlink(missing_ok=True)
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

        while not md_utils.is_fs_root_dir(current_dir):
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

        while not md_utils.is_fs_root_dir(current_dir):
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

    def write_version_info_to_db(self) -> Optional[Exception]:
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

            self.session.add(version_info_record)
            self.session.commit()
        except Exception as err:
            return err

        return None

    def touch(self, filepath: Path, debug: bool = False) -> None:
        """
        ...
        """
        assert filepath.is_absolute(), f"Expected absolute filepath. Got {filepath}"

        if not filepath.parent.exists():
            print(f"Directory {filepath.parent} doesn't exist. Abort.")
            sys.exit(1)

        branch_name = self.get_current_git_branch(filepath.parent)

        old_file_record = (
            self.session.query(FileORM).filter_by(filepath=str(filepath)).first()
        )

        maybe_errors: List[Optional[Exception]] = []

        # File doesn't exist it fs nor in the .md database.
        if not filepath.exists() and not old_file_record:
            maybe_err = self.create_new_file_record(
                session=self.session,
                filepath=filepath,
                branch_name=branch_name,
                file_exists=False,
            )
            maybe_errors.append(maybe_err)

        # file doesn't exist in md but exits in fs (i.e file wasnt created using md touch or due to branch switch)
        elif filepath.exists() and not old_file_record:
            maybe_err = self.create_new_file_record(
                session=self.session,
                filepath=filepath,
                branch_name=branch_name,
                file_exists=True,
            )
            maybe_errors.append(maybe_err)

        # file exists in md but not in fs (file was removed)
        elif not filepath.exists() and old_file_record:
            history_records = (
                self.session.query(HistoryORM)
                .filter_by(filepath=old_file_record.filepath)
                .all()
            )

            updated_filename, updated_filepath = (
                md_utils.get_filepath_with_delete_prefix(filepath)
            )

            # Update the File record.
            old_file_record.filename = updated_filename
            old_file_record.filepath = updated_filepath
            old_file_record.status = FileStatus.REMOVED
            old_file_record.timestamp_deleted = datetime.now()

            # Update assocaited history records.
            for history_record in history_records:
                history_record.filepath = updated_filepath

            # Remove hash file if it exists.
            maybe_err = self.remove_hash_file(filepath=filepath)
            maybe_errors.append(maybe_err)

            # Create new file and new .md record.
            # Note that "create_new_file" intentionally commits the staged changes
            # so that it can perform cleanup if necessary. This also commits
            # the above staged changes.
            maybe_err = self.create_new_file_record(
                session=self.session,
                filepath=filepath,
                branch_name=branch_name,
                file_exists=False,
            )
            maybe_errors.append(maybe_err)

        # file exists in both md and fs.
        elif filepath.exists() and old_file_record:
            maybe_errors = self.add_file_to_md(
                session=self.session,
                filepath=filepath,
                branch_name=branch_name,
            )

        self.session.close()

        errors = [err for err in maybe_errors if err is not None]
        if len(errors):
            for err in errors:
                print(err, file=sys.stderr)
                sys.exit(1)

    def untrack(self, filepath: Path) -> None:
        """
        Set file status to "UNTRACKED" if file is in "ACTIVE" state. Do nothing
        if file is already in "UNTRACKED" state. Otherwise fail.
        """

        if not filepath.exists():
            print(f"File {filepath.relative_to(Path.cwd())} doesn't exist. Abort.")
            sys.exit(1)

        file_record = self.session.query(FileORM).filter_by(filepath=filepath).first()
        if not file_record:
            print(
                f"File {filepath.relative_to(Path.cwd())} is not in md database.",
                file=sys.stderr,
            )
            sys.exit(2)

        if (
            file_record.status != FileStatus.ACTIVE
            and file_record.status != FileStatus.UNTRACKED
        ):
            print(
                f"Cannot change status of file that is in {file_record.status} state.",
                file=sys.stderr,
            )
            sys.exit(3)

        file_record.status = FileStatus.UNTRACKED
        self.session.commit()
        self.session.close()
        print(f"'untrack' {filepath.relative_to(Path.cwd())}")

    def remove_file(
        self,
        filepath: Path,
        purge: bool = False,
        force: bool = False,
        debug: bool = False,
    ) -> None:
        """
        Marks file in Mdm as REMOVED and removes the file from file system if the file exits.

        * If file doesn't exist in Mdm, it only removes the file from file system.
        * If file doesn't exist in file system, it marks it in Mdm as REMOVED if it has record in Mdm.
        * If file exists in file system but can't be removed by Mdm, the correspoding Mdm
          record won't be marked as REMOVED. -f/--force bypasses this check.

        Arguments
        filepath:   Filepath to the file.
        purge:      Removes all records associated with the file completely.
        force:      Removes Mdm records associated with the file even if Mdm is unable to remove
                    the file from file system. This only applies if the file exists. If file
                    doesn't exits then Mdm record will be removed automatically.
        debug:      Print error tracebacks to stderr together.

        Exit codes
        1           Failed to remove the file from file system. If file exists in file system but
                    can't be removed by Mdm, the correspoding Mdm record won't be marked as REMOVED.
        2           Failed to remove records from Mdm database.
        """
        stdout_messages: List[str] = []

        # Handle removing file from file system.
        if filepath.exists():
            try:
                filepath.unlink()
            except Exception:
                if not force:
                    if debug:
                        print(f"{traceback.format_exc()}\n", file=sys.stderr)

                    print(
                        f"Failed to delete {filepath}.\nUse --force to remove Mdm record anyway.",
                        file=sys.stderr,
                    )
                    sys.exit(1)

        # Handle removing file from Mdm database and hash file.
        try:
            file_record = (
                self.session.query(FileORM).filter_by(filepath=filepath).first()
            )
            history_records = (
                self.session.query(HistoryORM).filter_by(filepath=filepath).all()
            )

            if file_record:
                if purge:
                    self.session.delete(file_record)
                    for h_record in history_records:
                        self.session.delete(h_record)
                    # TODO: remove custom metadata records here
                    stdout_messages.append(f"'rm --purge'{filepath}")
                else:
                    updated_filename, updated_filepath = (
                        md_utils.get_filepath_with_delete_prefix(filepath=filepath)
                    )
                    file_record.status = FileStatus.REMOVED
                    file_record.filepath = updated_filepath
                    file_record.filename = updated_filename

                    for h_record in history_records:
                        h_record.filepath = updated_filepath
                    # TODO: update custom metadata records
                    stdout_messages.append(f"'rm' {filepath}")

                hash_filepath_or_err = self.get_path_to_hash_file(filepath=filepath)
                if isinstance(hash_filepath_or_err, Exception):
                    raise hash_filepath_or_err

                hash_filepath_or_err.unlink(missing_ok=True)

            self.session.commit()
            for message in stdout_messages:
                print(message)
        except Exception:
            if debug:
                print(f"{traceback.format_exc()}\n", file=sys.stderr)

            print(f"\nFailed to remove {filepath} from Mdm.", file=sys.stderr)
            sys.exit(2)

    def purge_removed_files(self, path: Path, debug: bool = False) -> None:
        """
        Purges files in 'removed' state from Mdm database.

        Arguments
        debug:      Print error tracebacks to stderr together.

        Exit codes
        1           Failed to purge the records from Mdm database.
        """

        try:
            deleted_file_records = (
                self.session.query(FileORM).filter_by(status=FileStatus.REMOVED).all()
            )

            for file_record in deleted_file_records:
                # TODO: purge custom file metadata once implemented
                history_records = (
                    self.session.query(HistoryORM)
                    .filter_by(filepath=file_record.filepath)
                    .all()
                )
                self.session.delete(file_record)
                for history_record in history_records:
                    self.session.delete(history_record)

            self.session.commit()
        except Exception:
            if debug:
                print(f"{traceback.format_exc()}\n", file=sys.stderr)

            print("Failed to purge removed files.", file=sys.stderr)
            sys.exit(1)

    def list_files(
        self, path: Path, status_filter: Optional[FileStatus] = None
    ) -> None:
        """
        TODO:
        List files:
            - by default list files in current directory
            - by default list only active files
            - --all can be used to list all files using relative paths to md repository root
            - --abs-path list files using their absolute paths
            - --untracked can be used to list untracked files
            - --removed can be used to list removed files
            - TODO: add option to search based on custom attributes and values
                    once the custom file attributes are implemented
        """
        assert path.is_absolute(), f"Expected absolute path. Got {path}"

        if status_filter:
            files = self.session.query(FileORM).filter_by(status=status_filter).all()
            for file in files:
                print(Path(file.filepath).relative_to(Path.cwd()))
        else:
            files = self.session.query(FileORM).all()
            for file in files:
                print(Path(file.filepath).relative_to(self.repository_root))
