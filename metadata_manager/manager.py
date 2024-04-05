import sys
import traceback
from pathlib import Path
from typing import Optional, List, Union
import subprocess
import shutil
from datetime import datetime
import uuid

from sqlalchemy import or_
from sqlalchemy.orm import Session

from db import get_session_or_exit
from md_models import (
    Config,
    FileORM,
    FileStatus,
    FileStat,
    HistoryORM,
    VersionInfo,
    VersionInfoORM,
    RepositoryORM,
    FileListing,
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

        md_path = path.joinpath(md_config.md_dir_name)
        db_path = md_path.joinpath(md_config.md_db_name)

        # If recreate flag is set, delete existing repository.
        if recreate and md_path.exists():
            try:
                shutil.rmtree(path.joinpath(md_config.md_dir_name))
            except Exception:
                if debug:
                    print(f"{traceback.format_exc()}\n", file=sys.stderr)

                print(f"Fatal: failed to recreate repository {path}.", file=sys.stderr)
                sys.exit(3)

        # Check if repository has been already initiazlied in target directory.
        if path.joinpath(md_config.md_dir_name).exists():
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

            # Create internal directories.
            md_path.mkdir()
            md_path.joinpath("deleted").mkdir()
            md_path.joinpath("hashes").mkdir()
        except Exception:
            if debug:
                print(f"{traceback.format_exc()}\n", file=sys.stderr)

            print(f"fatal: failed to initialize repository {path}", file=sys.stderr)
            sys.exit(1)

        repository = RepositoryORM(
            id=str(uuid.uuid4()),
            repository_filepath=path,
        )

        mdm = MetadataManager(
            md_config=md_config,
            repository_root=path,
            md_path=md_path,
            db_path=db_path,
        )

        session = get_session_or_exit(db_path=db_path)
        session.add(repository)

        maybe_err = mdm.write_version_info_to_db(session=session, commit=False)
        if maybe_err:
            mdm.cleanup(path)
            session.close()

            if debug:
                print(f"{traceback.format_exception(maybe_err)}\n", file=sys.stderr)

            print("fatal: failed to initialize repository", file=sys.stderr)
            sys.exit(1)

        session.commit()
        session.close()

        print(f"Intialized empty repository in {path}")
        return mdm

    @staticmethod
    def from_repository(md_config: Config, path: Path, debug: bool = False):
        assert path.is_absolute(), f"Expected aboslute path. Got {path}."

        maybe_md_root = md_utils.get_repository_root_or_exit(
            path=path, config=md_config
        )

        md_path = maybe_md_root.joinpath(md_config.md_dir_name)
        db_path = md_path.joinpath(md_config.md_db_name)

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

                source_session = get_session_or_exit(db_path=source_mdm.db_path)
                dest_session = get_session_or_exit(db_path=self.db_path)

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

            latest_history_record = HistoryORM.get_latest(session=session)
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
            errors: List[Optional[Exception]] = [err]
            maybe_err = self.remove_hash_file_or_dir(path=filepath)
            if maybe_err:
                errors.append(err)
            return errors

        return []

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
        parents: bool = False,
    ) -> None:
        """
        ...
        """
        assert filepath.is_absolute(), f"Expected absolute filepath. Got {filepath}"

        # Validate parent directory exists and create all parent directories as needed if
        # 'parent=True', otherwise exit.
        if not filepath.parent.exists():
            if parents:
                filepath.parent.mkdir(parents=True, exist_ok=True)
            else:
                session.close()
                print(
                    f"Fatal: Directory {filepath.parent} doesn't exist. Abort.",
                    file=sys.stderr,
                )
                print(
                    "\nprovide -p/--parents flag to create parent directories as needed.",
                    file=sys.stderr,
                )
                sys.exit(1)

        branch_name = self.get_current_git_branch(filepath.parent)

        old_file_record = (
            session.query(FileORM).filter_by(filepath=str(filepath)).first()
        )

        maybe_errors: List[Optional[Exception]] = []

        # File doesn't exist it fs nor in the .md database.
        if not filepath.exists() and not old_file_record:
            # Remove hash directory if it exists. This should ideally not be necessary
            # if all removal are handled via manager 'rm'. But in case they are removed via other
            # means, there will be dangling objects.
            # This specifically handles the case when originally there was a directory
            # with the same name as the file that is currently being created and it
            # was removed from fs.
            maybe_err = self.remove_hash_file_or_dir(path=filepath)
            maybe_errors.append(maybe_err)

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

        errors = [err for err in maybe_errors if err is not None]
        if len(errors):
            for err in errors:
                if debug:
                    print(f"{traceback.format_exception(err)}\n", file=sys.stderr)

                print(err, file=sys.stderr)

            session.close()
            sys.exit(1)

        print(f"touch: {filepath.relative_to(self.repository_root)}")

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
                    # TODO: remove custom metadata records here
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

                    for h_record in history_records:
                        h_record.filepath = updated_filepath
                    # TODO: update custom metadata records
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
                # TODO: purge custom file metadata once implemented
                history_records = (
                    session.query(HistoryORM)
                    .filter_by(filepath=file_record.filepath)
                    .all()
                )
                session.delete(file_record)
                for history_record in history_records:
                    session.delete(history_record)

            session.commit()
        except Exception:
            if debug:
                print(f"{traceback.format_exc()}\n", file=sys.stderr)

            session.close()
            print("Failed to purge removed files.", file=sys.stderr)
            sys.exit(1)

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
