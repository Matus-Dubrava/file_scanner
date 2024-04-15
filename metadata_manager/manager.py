import sys
import os
import traceback
from pathlib import Path
from typing import Optional, List, Union
import subprocess
import shutil
from datetime import datetime
import uuid

from sqlalchemy import or_, text
from sqlalchemy.orm import Session

from db import get_local_session_or_exit
from models.local_models import (
    Config,
    FileORM,
    FileMetadataORM,
    FileStatus,
    FileStat,
    HistoryORM,
    VersionInfo,
    VersionInfoORM,
    RepositoryORM,
    FileListing,
    RepositoryStats,
    RepositoryMetadataORM,
    LocalRefreshOutcome,
)
import md_utils


class MetadataManager:
    def __init__(
        self,
        md_config: Config,
        repository_root: Path,
        md_path: Path,
        db_path: Path,
    ):
        self.md_config = md_config
        self.repository_root = repository_root
        self.md_path = md_path
        self.db_path = db_path

    @staticmethod
    def new(md_config: Config, path: Path, recreate: bool = False, debug: bool = False):
        assert path.is_absolute(), f"Expected aboslute path. Got {path}."

        local_dir_path = path.joinpath(md_config.local_dir_name).resolve()
        local_db_path = local_dir_path.joinpath(md_config.local_db_name).resolve()

        # If recreate flag is set, delete existing repository.
        if recreate and local_dir_path.exists():
            try:
                shutil.rmtree(path.joinpath(md_config.local_dir_name))
            except Exception:
                if debug:
                    print(f"{traceback.format_exc()}\n", file=sys.stderr)

                print(f"Fatal: failed to recreate repository {path}.", file=sys.stderr)
                sys.exit(3)

        # Check if repository has been already initiazlied in target directory.
        if path.joinpath(md_config.local_dir_name).exists():
            print(f"Fatal: repository already exists in {path}", file=sys.stderr)
            print(
                (
                    "use (mdm init --recreate) to recreate repository, "
                    "this will delete the old repository and all data will be lost"
                ),
                file=sys.stderr,
            )
            sys.exit(2)

        try:
            # If specified directory doesn't exist. Create one.
            if not path.exists():
                path.mkdir(parents=True)

            # Create internal directories for local repository.
            local_dir_path.mkdir()
            local_dir_path.joinpath("hashes").mkdir()
        except Exception:
            if debug:
                print(f"{traceback.format_exc()}\n", file=sys.stderr)

            print(f"fatal: failed to initialize repository {path}", file=sys.stderr)
            sys.exit(1)

        try:
            local_repository_record = RepositoryORM(
                id=str(uuid.uuid4()),
                repository_filepath=path,
            )

            mdm = MetadataManager(
                md_config=md_config,
                repository_root=path,
                md_path=local_dir_path,
                db_path=local_db_path,
            )

            local_session = get_local_session_or_exit(
                db_path=local_db_path, debug=debug
            )
            local_session.add(local_repository_record)

            # Register local repository in global database.
            maybe_err = md_utils.register_local_repository(
                repository_id=local_repository_record.id,
                path=path,
                config=md_config,
            )
            if maybe_err:
                raise maybe_err

            maybe_err = mdm.write_version_info_to_db(
                session=local_session, commit=False
            )
            if maybe_err:
                raise maybe_err

            local_session.commit()
            local_session.close()
        except Exception:
            mdm.cleanup(path)
            local_session.close()

            if debug:
                print(f"{traceback.format_exc()}\n", file=sys.stderr)

            print("fatal: failed to initialize repository", file=sys.stderr)
            sys.exit(1)

        print(f"Intialized empty repository in {path}")
        return mdm

    @staticmethod
    def from_repository(md_config: Config, path: Path, debug: bool = False):
        assert path.is_absolute(), f"Expected aboslute path. Got {path}."

        maybe_md_root = md_utils.get_repository_root_or_exit(
            path=path, config=md_config
        )

        md_path = maybe_md_root.joinpath(md_config.local_dir_name)
        db_path = md_path.joinpath(md_config.local_db_name)

        assert md_path.exists(), f"Expected repository {path} to exist."

        # if path contains .md repo, load all necessary data from there
        return MetadataManager(
            md_config=md_config,
            repository_root=maybe_md_root,
            md_path=md_path,
            db_path=db_path,
        )

    def load_data_from_parent_repository(self, debug: bool = False) -> None:
        """
        Looks for parent Mdm resository and move all eligible objects. Store parent's repository id
        and filepath in current repository.

        * File record from source are copied over to destination and status of file in source is set to 'TRACKED_IN_SUBREPOSITORY'
        * History records are moved over to target Mdm.
        * Custom Metadata is moved over to target Mdm.
        """
        maybe_source_root = md_utils.get_repository_root(
            path=self.repository_root.parent, config=self.md_config
        )
        try:
            if maybe_source_root:
                source_mdm = MetadataManager.from_repository(
                    md_config=self.md_config, path=maybe_source_root
                )

                source_session = get_local_session_or_exit(db_path=source_mdm.db_path)
                dest_session = get_local_session_or_exit(db_path=self.db_path)

                source_repo_record = source_session.query(RepositoryORM).first()
                dest_repo_record = dest_session.query(RepositoryORM).first()
                assert (
                    source_repo_record and dest_repo_record
                ), "Expected repository records to exist."

                dest_repo_record.parent_repository_filepath = (
                    source_repo_record.repository_filepath
                )
                dest_repo_record.parent_repository_id = source_repo_record.id

                maybe_err = md_utils.move_mdm_data(
                    source_session=source_session,
                    dest_session=dest_session,
                    source_mdm=source_mdm,
                    dest_mdm=self,
                )
                if maybe_err:
                    raise maybe_err

                dest_session.commit()
                source_session.commit()
                dest_session.close()
                source_session.close()

                print("Succesfully loaded data from parent repository.")
            else:
                print("Couldn't find parent repository.")
        except Exception:
            if debug:
                print(f"{traceback.format_exc()}\n", file=sys.stderr)

            source_session.close()
            dest_session.close()
            print("Failed to load data from parent repository. Abort.", file=sys.stderr)
            sys.exit(2)

    def create_md_dirs(self, where: Path):
        (where / self.md_config.local_dir_name).mkdir()
        (where / self.md_config.local_dir_name / "deleted").mkdir()
        (where / self.md_config.local_dir_name / "hashes").mkdir()

    def cleanup(self, dir: Path):
        md_dir = dir / self.md_config.local_dir_name
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

    def remove_hash_file_or_dir(self, path: Path) -> Optional[Exception]:
        """
        Removes hash file from .md repository. Expect .md repository have
        been initilized. Path to hash file is computed.

        path:   path to the original file, not the hash file
        """
        hash_path_or_err = self.get_path_to_hash_file(filepath=path)
        if isinstance(hash_path_or_err, Exception):
            return hash_path_or_err

        if hash_path_or_err.is_dir():
            shutil.rmtree(hash_path_or_err)
        else:
            hash_path_or_err.unlink(missing_ok=True)
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
            if (current_dir / self.md_config.local_dir_name).exists():
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

    def create_repository_record(
        self,
        session: Session,
        filepath: Path,
        file_exists: bool,
        branch_name: Optional[str] = None,
    ) -> Optional[Exception]:
        try:
            # Track if file was created for the purposes of cleanup.
            file_was_created = False

            if not file_exists:
                filepath.touch()
                file_was_created = True

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
                id=str(uuid.uuid4()),
                filepath=str(filepath),
                version_control_branch=branch_name,
                fs_size=filepath.lstat().st_size if file_exists else 0,
                fs_inode=filepath.lstat().st_ino,
                timestamp_record_added=datetime.now(),
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
            if file_was_created:
                filepath.unlink()

            if isinstance(hash_filepath_or_err, Path):
                hash_filepath_or_err.unlink()

            return err

        return None

    def refresh_repository_record(
        self,
        session: Session,
        filepath: Path,
        branch_name: Optional[str] = None,
    ) -> Optional[List[Exception]]:
        """
        Refreshes exising repository file record. Adds new history record and
        recreates hash file.
        """
        errors: List[Exception] = []
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
            assert file_record, f"Expected file record for {filepath} to exist"
            file_record.version_control_branch = branch_name
            session.add(file_record)

            latest_history_record = HistoryORM.get_latest(
                session=session, filepath=file_record.filepath
            )
            assert latest_history_record, "Expected at least one history record."

            line_changes = md_utils.count_line_changes(
                old_hashes=hashes_or_err, new_hashes=file_stat_or_err.hashes
            )

            history_record = HistoryORM(
                id=str(uuid.uuid4()),
                filepath=str(filepath),
                version_control_branch=branch_name,
                fs_size=filepath.lstat().st_size,
                fs_inode=filepath.lstat().st_ino,
                timestamp_record_added=datetime.now(),
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
            errors.append(err)

            maybe_err = self.remove_hash_file_or_dir(path=filepath)
            if maybe_err:
                errors.append(err)

            return errors

        return errors if len(errors) else None

    def _refresh_active_repository_records(
        self, session: Session
    ) -> LocalRefreshOutcome:
        """
        Refreshes every active record - recomputes statistics, add new history records and recreates
        hash files.

        Returns refresh outcome:
        - list of paths that were succesfully refreshed
        - list of paths where refresh were unsuccessful, together with respective errors
        - other error if occured
        """
        refresh_stats = LocalRefreshOutcome.new()

        try:
            tracked_filepaths = [
                record.filepath
                for record in session.query(FileORM)
                .filter_by(status=FileStatus.ACTIVE)
                .all()
            ]
        except Exception as exc:
            refresh_stats.error = exc
            return refresh_stats

        for filepath in tracked_filepaths:
            maybe_errors = self.refresh_repository_record(
                session=session,
                filepath=Path(filepath),
                branch_name=self.get_current_git_branch(dir=self.repository_root),
            )

            if maybe_errors is not None:
                refresh_stats.add_failed_path(path=Path(filepath), errors=maybe_errors)
            else:
                refresh_stats.add_successful_path(path=Path(filepath))

        return refresh_stats

    def refresh_active_repository_records(
        self,
        session: Session,
        debug: bool = False,
        verbose: bool = False,
    ) -> None:
        """
        Refreshes every active record - recomputes statistics, add new history records and recreates
        hash files.
        """

        refresh_stats = self._refresh_active_repository_records(session=session)

        if refresh_stats.error:
            if debug:
                print(
                    f"{traceback.format_exception(refresh_stats.error)}\n",
                    file=sys.stderr,
                )

            print("fatal: refresh failed", file=sys.stderr)

        for failed_path in refresh_stats.failed_paths:
            if debug:
                for exc in failed_path.errors:
                    print(f"{traceback.format_exception(exc)}]n", file=sys.stderr)

            print(f"failed to refresh: {failed_path.path}", file=sys.stderr)

        if verbose:
            for successful_path in refresh_stats.successful_paths:
                print(f"refresh: {successful_path.relative_to(self.repository_root)}")
            print()

        print(f"refresh: {len(refresh_stats.successful_paths)} records")

    def write_version_info_to_db(
        self, session: Session, commit: bool = True
    ) -> Optional[Exception]:
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
            if commit:
                session.commit()
        except Exception as err:
            return err

        return None

    def touch(
        self,
        session: Session,
        filepath: Path,
        debug: bool = False,
        create_parents: bool = False,
    ) -> None:
        """
        ...
        """
        assert filepath.is_absolute(), f"Expected absolute filepath. Got {filepath}"
        assert (
            filepath.parent.exists() or create_parents
        ), f"Expected parent directory ({filepath.parent}) to exists or 'create_parents' flag to be set."

        # Validate parent directory exists and create all parent directories as needed if
        # 'create_parent=True'
        if not filepath.parent.exists() and create_parents:
            filepath.parent.mkdir(parents=True, exist_ok=True)

        branch_name = self.get_current_git_branch(filepath.parent)

        old_file_record = (
            session.query(FileORM).filter_by(filepath=str(filepath)).first()
        )

        errors: List[Exception] = []

        # File doesn't exist it fs nor in the .md database.
        if not filepath.exists() and not old_file_record:
            # Remove hash directory if it exists. This should ideally not be necessary
            # if all removal are handled via manager 'rm'. But in case they are removed via other
            # means, there will be dangling objects.
            # This specifically handles the case when originally there was a directory
            # with the same name as the file that is currently being created and it
            # was removed from fs.
            maybe_err = self.remove_hash_file_or_dir(path=filepath)
            if isinstance(maybe_err, Exception):
                errors.append(maybe_err)

            maybe_err = self.create_repository_record(
                session=session,
                filepath=filepath,
                branch_name=branch_name,
                file_exists=False,
            )
            if isinstance(maybe_err, Exception):
                errors.append(maybe_err)

        # file doesn't exist in md but exits in fs (i.e file wasnt created using md touch or due to branch switch)
        elif filepath.exists() and not old_file_record:
            maybe_err = self.create_repository_record(
                session=session,
                filepath=filepath,
                branch_name=branch_name,
                file_exists=True,
            )
            if isinstance(maybe_err, Exception):
                errors.append(maybe_err)

        # file exists in md but not in fs (file was removed)
        elif not filepath.exists() and old_file_record:
            history_records = (
                session.query(HistoryORM)
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

            # Remove hash file if it exists. This should ideally not be necessary
            # if all removal are handled via manager 'rm'. But in case they are removed via other
            # means, there will be dangling objects.
            maybe_err = self.remove_hash_file_or_dir(path=filepath)
            if isinstance(maybe_err, Exception):
                errors.append(maybe_err)

            # Create new file and new .md record.
            # Note that "create_new_file" intentionally commits the staged changes
            # so that it can perform cleanup if necessary. This also commits
            # the above staged changes.
            maybe_err = self.create_repository_record(
                session=session,
                filepath=filepath,
                branch_name=branch_name,
                file_exists=False,
            )
            if isinstance(maybe_err, Exception):
                errors.append(maybe_err)

        # file exists in both md and fs.
        elif filepath.exists() and old_file_record:
            maybe_errors = self.refresh_repository_record(
                session=session,
                filepath=filepath,
                branch_name=branch_name,
            )
            if maybe_errors is not None:
                errors.extend(maybe_errors)

        if len(errors):
            for err in errors:
                if debug:
                    print(f"{traceback.format_exception(err)}\n", file=sys.stderr)

                print(err, file=sys.stderr)

            session.close()
            sys.exit(1)

        print(f"touch: {filepath.relative_to(self.repository_root)}")

    def add_file(self, session: Session, filepath: Path, debug: bool = False) -> None:
        """
        Start tracking provided file - set its status to 'ACTIVE'.
        If file is already being tracked, do nothing.
        """

        assert filepath.exists(), f"Expected file '{filepath}' to exist."
        assert (
            filepath.is_file()
        ), f"Expected provided filepath '{filepath}' to be file."

        branch_name = self.get_current_git_branch(filepath.parent)

        if not session.query(FileORM).filter_by(filepath=filepath).first():
            maybe_err = self.create_repository_record(
                session=session,
                filepath=filepath,
                branch_name=branch_name,
                file_exists=True,
            )

            if maybe_err:
                if debug:
                    print(f"{traceback.format_exc()}\n", file=sys.stderr)

                print(maybe_err, file=sys.stderr)
                session.close()
                exit(1)

            print(f"tracking: {filepath}")

    def add_directory(
        self, session: Session, dirpath: Path, debug: bool = False
    ) -> None:
        """
        Traverse directory together with all nested subdirectories and start tracking
        all files that were found.
        """
        assert dirpath.exists(), f"Expected file '{dirpath}' to exist."
        assert dirpath.is_dir(), f"Expected provided dirpath '{dirpath}' to be file."

        for rootdir, _, filenames in os.walk(dirpath):
            for filename in filenames:
                self.add_file(
                    session=session,
                    filepath=Path(rootdir).joinpath(filename),
                    debug=debug,
                )

    def untrack(self, session: Session, filepath: Path) -> None:
        """
        Set file status to "UNTRACKED" if file is in "ACTIVE" state. Do nothing
        if file is already in "UNTRACKED" state. Otherwise fail.
        """

        if not filepath.exists():
            session.close()
            print(f"File {filepath.relative_to(Path.cwd())} doesn't exist. Abort.")
            sys.exit(1)

        file_record = session.query(FileORM).filter_by(filepath=filepath).first()
        if not file_record:
            session.close()
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
        session.commit()
        print(f"'untrack' {filepath.relative_to(Path.cwd())}")

    def remove_file(
        self,
        session: Session,
        filepath: Path,
        purge: bool = False,
        force: bool = False,
        debug: bool = False,
        keep_local: bool = False,
    ) -> None:
        """
        Sets file status of the provided file to REMOVED and removes it from file system if the file exists.

        If file exists in file system but can't be removed, the correspoding
        record won't be marked as REMOVED if 'force' isn't set to True.

        Arguments
        filepath:   File to be removed.
        purge:      Removes all records associated with the file completely.
        force:      Removes records associated with the file even if Mdm is unable to remove
                    the file from file system. This only applies if the file exists. If file
                    doesn't exits then Mdm record will be removed automatically.
        debug:      Print error tracebacks to stderr together with custom error messages.
        keep_local: Remove database records and hash object related to the file but don't remove the file.
        """
        # Handle removing file from internal database and hash file.
        try:
            file_record = session.query(FileORM).filter_by(filepath=filepath).first()
            history_records = (
                session.query(HistoryORM).filter_by(filepath=filepath).all()
            )
            metadata_records = (
                session.query(FileMetadataORM).filter_by(filepath=filepath).all()
            )

            # Can't remove files that are not within internal database.
            if not file_record:
                session.close()
                print(
                    f"fatal: path {filepath} did not match any tracked file",
                    file=sys.stderr,
                )
                sys.exit(3)

            else:
                # Handle removing file from file system.
                # Continue to remove file from repository if file doesn't exits.
                if filepath.exists() and not keep_local:
                    try:
                        filepath.unlink()
                    except Exception:
                        if not force:
                            if debug:
                                print(f"{traceback.format_exc()}\n", file=sys.stderr)

                            print(
                                f"fatal: failed to delete {filepath} from file system.\n\nuse --force to remove record anyway",
                                file=sys.stderr,
                            )
                            sys.exit(1)

                stdout_message = ""
                if purge:
                    # Delete records associated with the file from database completely.
                    session.delete(file_record)
                    for h_record in history_records:
                        session.delete(h_record)

                    # Delete key/value pairs associated with the file.
                    for metadata_record in metadata_records:
                        session.delete(metadata_record)

                    stdout_message = f"'rm --purge'{filepath}"
                else:
                    # Set the file status to REMOVED, mangle its filepath and update existing records assicated
                    # with the file to reflect this filepath change.
                    updated_filename, updated_filepath = (
                        md_utils.get_filepath_with_delete_prefix(filepath=filepath)
                    )
                    file_record.status = FileStatus.REMOVED
                    file_record.filepath = updated_filepath
                    file_record.filename = updated_filename

                    # Updated file's history record.
                    for h_record in history_records:
                        h_record.filepath = updated_filepath

                    # Update file's key/value pair records.
                    for metadata_record in metadata_records:
                        metadata_record.filepath = updated_filepath

                    stdout_message = f"rm: {filepath}"

                hash_filepath_or_err = self.get_path_to_hash_file(filepath=filepath)
                if isinstance(hash_filepath_or_err, Exception):
                    raise hash_filepath_or_err

                hash_filepath_or_err.unlink(missing_ok=True)

                session.commit()
                if stdout_message:
                    print(stdout_message)
        except Exception:
            if debug:
                print(f"{traceback.format_exc()}\n", file=sys.stderr)

            session.close()
            print(f"fatal: failed to remove {filepath}", file=sys.stderr)
            sys.exit(2)

    def remove_files(
        self,
        session: Session,
        filepaths: List[Path],
        purge: bool = False,
        force: bool = False,
        debug: bool = False,
        keep_local: bool = False,
    ) -> None:
        """
        Sets file status of provided files to REMOVED and removes them from file system if they exist.
        All provided files must be present in repository, otherwise this function exits without removing
        any file.

        If file exists in file system but can't be removed, the correspoding
        record won't be marked as REMOVED if 'force' isn't set to True.

        Arguments
        filepaths:  Files to be removed.
        purge:      Removes all records associated with the file completely.
        force:      Removes records associated with the file even if Mdm is unable to remove
                    the file from file system. This only applies if the file exists. If file
                    doesn't exits then Mdm record will be removed automatically.
        debug:      Print error tracebacks to stderr together with custom error messages.
        """
        for filepath in filepaths:
            filepath = filepath.resolve()

            # Confirm that all provded paths are files if they exist.
            if filepath.exists() and not filepath.is_file():
                print(f"fatal: path {filepath} is not a file.", file=sys.stderr)
                sys.exit(4)

            # Confirm all provided files are present in repository. Exit as soon as
            # one of them is missing.
            if not session.query(FileORM).filter_by(filepath=filepath).first():
                print(
                    f"fatal: path {filepath} did not match any tracked file",
                    file=sys.stderr,
                )
                sys.exit(3)

        # Remove all files. Intentially in a separate loop so that no files are removed
        # if previous checks fail for any file.
        for filepath in filepaths:
            self.remove_file(
                session=session,
                filepath=filepath,
                purge=purge,
                force=force,
                debug=debug,
                keep_local=keep_local,
            )

    def purge_removed_files(
        self, session: Session, path: Path, debug: bool = False
    ) -> None:
        """
        Purges files in 'removed' state from Mdm database.

        Arguments
        debug:      Print error tracebacks to stderr together.

        Exit codes
        1           Failed to purge the records from Mdm database.
        """

        try:
            deleted_file_records = (
                session.query(FileORM).filter_by(status=FileStatus.REMOVED).all()
            )

            for file_record in deleted_file_records:
                history_records = (
                    session.query(HistoryORM)
                    .filter_by(filepath=file_record.filepath)
                    .all()
                )
                metadata_records = (
                    session.query(FileMetadataORM)
                    .filter_by(filepath=file_record.filepath)
                    .all()
                )

                session.delete(file_record)
                for history_record in history_records:
                    session.delete(history_record)
                for metadata_record in metadata_records:
                    session.delete(metadata_record)

            session.commit()
        except Exception:
            if debug:
                print(f"{traceback.format_exc()}\n", file=sys.stderr)

            session.close()
            print("Failed to purge removed files.", file=sys.stderr)
            sys.exit(1)

        print(f"purged {len(deleted_file_records)} records")

    def _list_files(
        self, session: Session, status_filter: List[FileStatus]
    ) -> List[FileORM]:
        status_filter_condition = or_(
            *[FileORM.status == status for status in status_filter]
        )

        return session.query(FileORM).where(status_filter_condition).all()

    def list_files(
        self,
        session: Session,
        path: Path,
        status_filter: List[FileStatus] = [FileStatus.ACTIVE],
        abs_paths: bool = False,
        no_header: bool = False,
        dump_json_path: Optional[Path] = None,
        debug: bool = False,
        force: bool = False,
    ) -> None:
        """
        List files in tracked in repository.
        """
        assert path.is_absolute(), f"Expected absolute path. Got {path}"

        repository_record = session.query(RepositoryORM).first()
        assert repository_record, "Expected repository record to exist."

        file_records = self._list_files(session=session, status_filter=status_filter)

        if dump_json_path:
            file_listing = FileListing(
                repository_id=repository_record.id,
                repository_path=repository_record.repository_filepath,
                applied_status_filters=status_filter,
                filepaths=[record.filepath for record in file_records],
            )

            try:
                # Try to create parent directories.
                if force:
                    dump_json_path.parent.mkdir(parents=True, exist_ok=True)

                with open(dump_json_path, "w") as f:
                    f.write(file_listing.model_dump_json())
            except Exception:
                if debug:
                    print(f"{traceback.format_exc()}\n", file=sys.stderr)

                session.close()
                print(
                    f"fatal: failed to write result to {dump_json_path}",
                    file=sys.stderr,
                )
                sys.exit(1)
        else:
            if not no_header:
                print(f"Repository id:\t\t{repository_record.id}")
                print(f"Repository path:\t{repository_record.repository_filepath}")
                print()
                print(
                    f"listing files with status: [{', '.join([status.to_str() for status in status_filter])}]\n"
                )

            for file_record in file_records:
                if abs_paths:
                    print(Path(file_record.filepath).absolute())
                else:
                    print(Path(file_record.filepath).relative_to(Path.cwd()))

    def compute_repository_statistics(
        self, session: Session
    ) -> RepositoryStats | Exception:
        """
        Count the number of active files, removed files, total lines, total lines added
        total line removed.
        """

        try:
            n_active_files = (
                session.query(FileORM).filter_by(status=FileStatus.ACTIVE).count()
            )
            n_removed_files = (
                session.query(FileORM).filter_by(status=FileStatus.REMOVED).count()
            )

            sql = """
                SELECT 
                    SUM(count_total_lines) as total_count_lines,
                    SUM(running_added_lines) as total_lines_added,
                    SUM(running_removed_lines) as total_lines_removed
                FROM (
                    SELECT 
                        *,
                        MAX(timestamp_record_added) OVER (
                            PARTITION BY filepath 
                            ORDER BY timestamp_record_added DESC
                        ) AS max_timestamp_record_added
                    FROM history 
                )
                WHERE 
                    timestamp_record_added = max_timestamp_record_added AND
                    filepath IN (
                        SELECT filepath 
                        FROM file 
                        WHERE status_enum = 'ACTIVE'
                    );
            """
            result = session.execute(text(sql)).fetchone()

            return RepositoryStats(
                active_files_count=n_active_files,
                removed_files_count=n_removed_files,
                total_lines_count=int(result[0]) if result and result[0] else 0,
                added_lines_count=int(result[1]) if result and result[1] else 0,
                removed_lines_count=int(result[2]) if result and result[2] else 0,
            )
        except Exception as exc:
            return exc

    def show_repository(self, session: Session, debug: bool = False) -> None:
        """
        Show repository information.
        """

        repository_record = session.query(RepositoryORM).first()

        if not repository_record:
            print("fatal: not a repository", file=sys.stderr)
            sys.exit(1)

        repository_stats = self.compute_repository_statistics(session=session)
        if isinstance(repository_stats, Exception):
            if debug:
                print(
                    f"{traceback.format_exception(repository_stats)}\n",
                    file=sys.stderr,
                )

            print("fatal: failed to compute repositry statistics", file=sys.stderr)
            sys.exit(2)

        repository_record.pretty_print()
        print()
        repository_stats.pretty_print()

    def show_file(
        self,
        session: Session,
        filepath: Path,
        display_metadata: bool = False,
        display_history: bool = False,
        display_n_history_records: Optional[int] = None,
        debug: bool = False,
    ) -> None:
        """
        Show file information.
        """

        file_record = session.query(FileORM).filter_by(filepath=filepath).first()

        if not file_record:
            print(
                f"fatal: {filepath.relative_to(self.repository_root)} is not tracked",
                file=sys.stderr,
            )
            sys.exit(1)

        file_record.pretty_print(session=session)

        if display_metadata:
            metadata_records = (
                session.query(FileMetadataORM).filter_by(filepath=filepath).all()
            )

            print()
            print("--------")
            print("METADATA")
            print("--------")
            print()

            for metadata_record in metadata_records:
                print(f"{metadata_record.key}:{metadata_record.value}")

        if display_history:
            if display_n_history_records:
                history_records = (
                    session.query(HistoryORM)
                    .filter_by(filepath=filepath)
                    .order_by(HistoryORM.timestamp_record_added.desc())
                    .limit(display_n_history_records)
                )
            else:
                history_records = (
                    session.query(HistoryORM)
                    .filter_by(filepath=filepath)
                    .order_by(HistoryORM.timestamp_record_added.desc())
                )

            for record in history_records:
                print()
                print("-------")
                print("HISTORY")
                print("-------")
                print()
                record.pretty_print()

    def set_value(
        self,
        session: Session,
        key: str,
        value: str,
        filepath: Optional[Path] = None,
        debug: bool = False,
    ) -> None:
        """
        Sets metadata key/value for a given file if filepath is provided. Otherwise sets
        it at repository level.
        """

        try:
            if not filepath:
                repository_record = (
                    session.query(RepositoryMetadataORM).filter_by(key=key).first()
                )
                if repository_record:
                    repository_record.value = value
                else:
                    repository_record = RepositoryMetadataORM(key=key, value=value)
                    session.add(repository_record)
            else:
                file_record = (
                    session.query(FileMetadataORM)
                    .filter_by(filepath=filepath, key=key)
                    .first()
                )
                if file_record:
                    file_record.value = value
                else:
                    file_record = FileMetadataORM(
                        filepath=filepath, key=key, value=value
                    )
                    session.add(file_record)

            session.commit()
        except Exception:
            if debug:
                print(f"{traceback.format_exc()}\n", file=sys.stderr)

            print("fatal: failed to set value", file=sys.stderr)
            sys.exit(1)

    def delete_key(
        self,
        session: Session,
        key: str,
        filepath: Optional[Path] = None,
        debug: bool = False,
    ) -> None:
        """
        Remove key/value associated with the filepath if provided, otherwise remove it
        from repository.
        """

        try:
            if not filepath:
                repository_record = (
                    session.query(RepositoryMetadataORM).filter_by(key=key).first()
                )
                if repository_record:
                    session.delete(repository_record)
            else:
                file_record = (
                    session.query(FileMetadataORM)
                    .filter_by(filepath=filepath, key=key)
                    .first()
                )
                if file_record:
                    session.delete(file_record)

            session.commit()
        except Exception:
            if debug:
                print(f"{traceback.format_exc()}\n", file=sys.stderr)

            print(f"fatal: failed to remove key '{key}'", file=sys.stderr)
            sys.exit(1)

    def get_value(
        self,
        session: Session,
        key: str,
        filepath: Optional[Path],
        get_all: bool = False,
        filter_key: Optional[str] = None,
        filter_value: Optional[str] = None,
        debug: bool = False,
    ) -> None:
        """
        Retrieve the value associated with the given key.
        If a filepath is provided, fetch the value associated with that file.
        Otherwise, obtain the value associated with the repository.
        """

        try:
            # Filter was provided, search through all file records.
            if filter_key or filter_value:
                if filter_key and filter_value:
                    file_records = (
                        session.query(FileMetadataORM)
                        .filter_by(key=filter_key, value=filter_value)
                        .all()
                    )
                elif filter_key:
                    file_records = (
                        session.query(FileMetadataORM).filter_by(key=filter_key).all()
                    )
                elif filter_value:
                    file_records = (
                        session.query(FileMetadataORM)
                        .filter_by(value=filter_value)
                        .all()
                    )
                else:
                    raise Exception("Unreachable.")

                for file_rec in file_records:
                    print(Path(file_rec.filepath).relative_to(self.repository_root))
            # Filepath was not provided. Fetch the value associated with repository.
            elif not filepath:
                if get_all:
                    repository_records = session.query(RepositoryMetadataORM).all()
                    for repository_rec in repository_records:
                        print(f"{repository_rec.key}: {repository_rec.value}")
                else:
                    repository_record = (
                        session.query(RepositoryMetadataORM).filter_by(key=key).first()
                    )
                    if repository_record:
                        print(repository_record.value)
            # Get value(s) associated with file.
            else:
                if get_all:
                    file_records = (
                        session.query(FileMetadataORM)
                        .filter_by(filepath=filepath)
                        .all()
                    )
                    for file_rec in file_records:
                        print(f"{file_rec.key}: {file_rec.value}")
                else:
                    file_record = (
                        session.query(FileMetadataORM)
                        .filter_by(filepath=filepath, key=key)
                        .first()
                    )
                    if file_record:
                        print(file_record.value)

        except Exception:
            if debug:
                print(f"{traceback.format_exc()}\n", file=sys.stderr)

            print(
                f"fatal: failed to fetch value associated with key '{key}'",
                file=sys.stderr,
            )
            sys.exit(1)
