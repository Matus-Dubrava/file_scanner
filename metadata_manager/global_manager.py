from typing import List, Tuple, Dict
from datetime import datetime
import uuid
import time
import traceback
import sys
import logging
from pathlib import Path

from sqlalchemy.orm import Session

from models.local_models import (
    Config,
    GlobalRefreshOutcome,
    LocalRefreshOutcome,
    HistoryORM,
)
from models.global_models import (
    RepositoriesORM,
    RefreshLogORM,
    RefreshRepositoryORM,
    RefreshFileORM,
)
import md_utils
from manager import MetadataManager
from db import LocalSession, GlobalSessionOrExit
from md_constants import YELLOW, GREEN, RED, RESET


class GlobalManager:
    def __init__(self, config: Config, debug: bool = False):
        self.config = config
        self.dir_path, self.db_path, self.log_dir = md_utils.get_global_paths(config)

        self.debug_logger = self._get_logger_or_exit(log_dir=self.log_dir, debug=debug)

        # Initalize global database if it doesn't exists.
        with GlobalSessionOrExit(db_path=self.db_path):
            pass

    def _get_logger_or_exit(self, log_dir: Path, debug: bool = False) -> logging.Logger:
        """
        Returns logger of exits if provided log directory is not a directory
        or new one can't be created.

        log_dir:    Log directory.
        debug:      Display debug information.
        """

        debug_logger_or_err = md_utils.get_logger(log_dir=log_dir)
        if isinstance(debug_logger_or_err, Exception):
            print(f"fatal: {debug_logger_or_err}")
            if debug:
                traceback.print_exception(debug_logger_or_err)

            sys.exit(1)

        return debug_logger_or_err

    def _is_valid_repository(self, path: Path) -> bool:
        """
        Checks if repository exists at given path.

        Note: This can be extended to perform more complex validation.

        path:   Repository path.
        """

        return path.joinpath(self.config.local_dir_name).exists()

    def _list_repositories(
        self, session: Session, all_: bool = False
    ) -> List[RepositoriesORM] | Exception:
        """
        Returns list of repositories.

        session:    Global database session.
        all_:       Return only repositories that exists in FS. If repository is deleted,
                    its record in database is not automatically removed.
        """
        try:
            repos = session.query(RepositoriesORM).all()

            if all_:
                return repos
            else:
                return [
                    repo for repo in repos if self._is_valid_repository(Path(repo.path))
                ]
        except Exception as exc:
            return exc

    def _list_repositories_or_exit(
        self, session: Session, all_: bool = False, debug: bool = False
    ) -> List[RepositoriesORM]:
        """
        Returns list of repositories or exit.

        session:    Global database session.
        all_:       Return only repositories that exists in FS. If repository is deleted,
                    its record in database is not automatically removed.
        debug:      Display debug information.
        """
        repos = self._list_repositories(session=session, all_=all_)

        if isinstance(repos, Exception):
            traceback_msg = traceback.format_exception(repos)
            error_msg = "fatal: failed to list repositories"

            if debug:
                print(f"{traceback_msg}\n", file=sys.stderr)

            print(error_msg, file=sys.stderr)

            md_utils.log_message(
                logger=self.debug_logger,
                log_level=logging.ERROR,
                message=error_msg,
                error=repos,
            )

            sys.exit(1)

        return repos

    def list_repositories(
        self, session: Session, debug: bool = False, all_: bool = False
    ) -> None:
        """
        Displays list of repositories.

        session:    Global database session.
        debug:      Display debug information.
        all_:       Return only repositories that exists in FS. If repository is deleted,
                    its record in database is not automatically removed.
        """
        repos = self._list_repositories_or_exit(session=session, all_=all_, debug=debug)

        for repo in repos:
            print(f"[{repo.id}] {repo.path}")

    def _get_refresh_files_from_refresh_outcome(
        self, refresh_outcome: LocalRefreshOutcome, repository_path: Path
    ) -> Tuple[List[RefreshFileORM], List[Exception]]:
        """
        Processes local refresh outcome and creates corresponding instances of "RefreshFileOMR".

        Individual file refresh statistics are fetched from respective local repository databases.
        """

        refresh_files: List[RefreshFileORM] = []
        errors: List[Exception] = []

        for path in refresh_outcome.successful_paths:
            try:
                local_db_path = repository_path.joinpath(
                    self.config.local_dir_name, self.config.local_db_name
                )
                with LocalSession(db_path=local_db_path) as local_session:
                    latest_history_record = HistoryORM.get_latest(
                        session=local_session, filepath=path
                    )
                    assert (
                        latest_history_record
                    ), "History record should exist at this point."

                    refresh_file = RefreshFileORM(
                        path=path,
                        lines_added=latest_history_record.count_added_lines,
                        lines_removed=latest_history_record.count_removed_lines,
                        running_lines_added=latest_history_record.running_added_lines,
                        running_lines_removed=latest_history_record.running_removed_lines,
                    )
                    refresh_files.append(refresh_file)
            except Exception as exc:
                refresh_file = RefreshFileORM(
                    path=path,
                    error_occured=1,
                    error=str(exc),
                    error_tb=",".join(traceback.format_exc()).replace("\n", " "),
                )
                refresh_files.append(refresh_file)
                errors.append(exc)

        for path_with_error in refresh_outcome.failed_paths:
            refresh_file = RefreshFileORM(
                path=path_with_error.path,
                error=",".join(str(error) for error in path_with_error.errors),
                error_occured=1,
                error_tb="||".join(
                    [
                        ",".join(traceback.format_exception(exc)).replace("\n", " ")
                        for exc in path_with_error.errors
                    ]
                ),
            )
            refresh_files.append(refresh_file)

        return refresh_files, errors

    def _populate_refresh_database_records(
        self,
        global_session: Session,
        global_refresh_outcome: GlobalRefreshOutcome,
        refresh_repositories: List[RefreshRepositoryORM],
        refresh_files_by_repository: Dict[str, List[RefreshFileORM]],
    ) -> Exception | None:
        """
        Populate refresh database records: log, repositories and files.
        """

        try:
            refresh_log_id = str(uuid.uuid4())
            refresh_log = RefreshLogORM(
                id=refresh_log_id,
                taken_at=datetime.now(),
                duration=global_refresh_outcome.duration,  # type: ignore
                total_repositories=global_refresh_outcome.total_repositories,
                repositories_refreshed=global_refresh_outcome.refreshed_repositories,
                repositories_refreshed_with_errors=global_refresh_outcome.refreshed_repositories_with_errors,
                repositories_failed=global_refresh_outcome.failed_repositories,
            )
            global_session.add(refresh_log)

            for refresh_repo in refresh_repositories:
                refresh_repo_id = str(uuid.uuid4())
                refresh_repo.id = refresh_repo_id
                refresh_repo.refresh_id = refresh_log_id
                global_session.add(refresh_repo)

                refresh_repo_files = refresh_files_by_repository.get(
                    refresh_repo.repository_id
                )

                # Refresh file records might not exists if we were unable
                # to read from local repository database. ex: database is missing
                # or corrupted.
                if refresh_repo_files:
                    for refresh_file in refresh_repo_files:
                        refresh_file.id = str(uuid.uuid4())
                        refresh_file.refresh_repository_id = refresh_repo.id
                        global_session.add(refresh_file)

            global_session.commit()
        except Exception as exc:
            return exc

        return None

    def _log_and_display_paths_with_errors(
        self,
        repository_path: Path | str,
        repository_refresh_outcome: LocalRefreshOutcome,
        debug: bool = False,
        verbose: bool = False,
    ) -> None:
        """
        Logs and displays paths that encountered errors during refresh.
        By default, result is logged only to log file. Use 'debug' and/or 'verbose'
        to log to standard output as well.

        repository_path:                Repository's root directory.
        repository_refresh_outcome:     Refresh outcome of local repository.
        debug:                          Display all debug information.
        verbose:                        Display only failed paths without error tracebacks.
        """

        print(f"{repository_path} {YELLOW}[refreshed with errors]{RESET}")
        md_utils.log_message(
            logger=self.debug_logger,
            log_level=logging.WARNING,
            message=f"{repository_path} [refreshed with errors]",
        )

        if verbose or debug:
            for path_with_errors in repository_refresh_outcome.failed_paths:
                for error in path_with_errors.errors:
                    print(f"  {RED}[failed]{RESET}\t{path_with_errors.path}")

                    if debug:
                        traceback.print_exception(error, file=sys.stderr)

        for path_with_errors in repository_refresh_outcome.failed_paths:
            for error in path_with_errors.errors:
                md_utils.log_message(
                    logger=self.debug_logger,
                    log_level=logging.ERROR,
                    message=f"[failed] {path_with_errors.path}",
                    error=error,
                )

    def _log_and_display_repository_level_refresh_error(
        self,
        repository_path: str | Path,
        error: Exception,
        debug: bool = False,
    ):
        """
        Logs and displays error that occured at during refresh at repository level,
        meaning that something was wrong with the respoitory itself, not with individual
        files.

        repository_path:                Repository's root directory.
        error:                          Error encountered while processing repository.
        debug:                          Display all debug information.
        """

        print(
            f"{repository_path} {RED}[failed to refresh]{RESET}",
            file=sys.stderr,
        )

        if debug:
            traceback.print_exception(error, file=sys.stderr)

        md_utils.log_message(
            logger=self.debug_logger,
            log_level=logging.ERROR,
            message=f"{repository_path} [failed to refresh]",
            error=error,
        )

    def _log_and_display_repository_refresh_success_message(
        self, repository_path: str | Path
    ) -> None:
        """
        Logs and displays repository refresh success message.

        repository_path:    Repository's root directory.
        """

        print(f"{repository_path} {GREEN}[refreshed]{RESET}")
        md_utils.log_message(
            logger=self.debug_logger,
            log_level=logging.INFO,
            message=f"{repository_path} [refreshed]",
        )

    def _log_and_display_successful_paths(
        self, repository_refresh_outcome: LocalRefreshOutcome, verbose: bool = False
    ) -> None:
        """
        Logs successfully refreshed paths to DEBUG log file and optionally to console.

        repository_refresh_outcome:     Refresh outcome of local repository.
        verbose:                        Print successful paths to conosle.
        """

        if repository_refresh_outcome.successful_paths and verbose:
            for path in repository_refresh_outcome.successful_paths:
                print(f"  {GREEN}[successful]{RESET}\t{path}")

        if repository_refresh_outcome.successful_paths:
            for path in repository_refresh_outcome.successful_paths:
                md_utils.log_message(
                    logger=self.debug_logger,
                    log_level=logging.DEBUG,
                    message=f"[successful] {path}",
                )

    def _log_and_display_global_refresh_summary(
        self,
        global_refresh_outcome: GlobalRefreshOutcome,
        duration: float,
        errors_while_getting_file_statistics: List[Exception],
        error_writing_result: Exception | None = None,
        debug: bool = False,
    ) -> None:
        """
        Logs and displays global refresh summary statistics and errors encountered during refresh.

        global_refresh_outcome:                 Outcome of global refresh.
        duration:                               Refresh duration.
        errors_while_getting_file_statistics:   Errors encoundered while computing file statistics.
        error_writing_result:                   Error encountered while writing refresh outcome to database.
        debug:                                  Display debug information.
        """

        print()
        md_utils.print_centered_message(
            message=f" refresh summary (in {duration}s) ", filler_char="=", new_lines=1
        )
        global_refresh_outcome.pretty_print()

        md_utils.log_message(
            logger=self.debug_logger,
            log_level=logging.INFO,
            message=global_refresh_outcome.get_log_str(),
        )
        md_utils.log_message(
            logger=self.debug_logger,
            log_level=logging.INFO,
            message=f"global refresh finished in {duration}s",
        )

        if errors_while_getting_file_statistics:
            if debug:
                print("errors while getting files statitics:", file=sys.stderr)
                for error in errors_while_getting_file_statistics:
                    traceback.print_exception(error, file=sys.stderr)

            for error in errors_while_getting_file_statistics:
                md_utils.log_message(
                    logger=self.debug_logger,
                    log_level=logging.ERROR,
                    message="error while getting file statitics",
                    error=error,
                )

        if error_writing_result:
            if debug:
                traceback.print_exception(error_writing_result, file=sys.stderr)

            print("\nerror: failed to write refresh records to database")

            md_utils.log_message(
                logger=self.debug_logger,
                log_level=logging.ERROR,
                message="error: failed to write refresh records to database",
                error=error_writing_result,
            )

    def refresh_all_repositories(
        self, session: Session, debug: bool = False, verbose: bool = False
    ) -> GlobalRefreshOutcome:
        """
        Refresh all records in all valid repositories.

        session:    Global database session.
        debug:      Display debug information.
        verbose:    Display path of every file that is being refreshed.
        """

        start = time.time()

        md_utils.log_message(
            logger=self.debug_logger,
            log_level=logging.INFO,
            message="start global refresh",
        )

        valid_repos = self._list_repositories_or_exit(session=session)

        errors_while_getting_file_statistics: List[Exception] = []

        refresh_outcome = GlobalRefreshOutcome.new()
        refresh_outcome.total_repositories = len(valid_repos)
        refresh_repositories: List[RefreshRepositoryORM] = []
        repository_error: str | None = None
        repository_error_tb: str | None = None
        repository_refresh_outcome = LocalRefreshOutcome.new()
        refresh_files_by_repository: Dict[str, List[RefreshFileORM]] = {}

        md_utils.print_centered_message(
            message=" repositories ", filler_char="=", new_lines=1
        )

        for repo in valid_repos:
            try:
                mdm = MetadataManager.from_repository(
                    md_config=self.config, path=Path(repo.path), debug=debug
                )

                with LocalSession(db_path=mdm.db_path) as local_session:
                    repository_refresh_outcome = mdm._refresh_active_repository_records(
                        session=local_session
                    )

                    refresh_files, errors = (
                        self._get_refresh_files_from_refresh_outcome(
                            refresh_outcome=repository_refresh_outcome,
                            repository_path=Path(repo.path),
                        )
                    )
                    errors_while_getting_file_statistics.extend(errors)
                    refresh_files_by_repository[repo.id] = refresh_files

                    refresh_outcome.failed_files += len(
                        repository_refresh_outcome.failed_paths
                    )
                    refresh_outcome.refreshed_files += len(
                        repository_refresh_outcome.successful_paths
                    )

                    # Repository-level error.
                    if repository_refresh_outcome.error is not None:
                        refresh_outcome.failed_repositories += 1
                        self._log_and_display_repository_level_refresh_error(
                            repository_path=repo.path,
                            error=repository_refresh_outcome.error,
                            debug=debug,
                        )

                    # File-specific errors.
                    if repository_refresh_outcome.failed_paths:
                        refresh_outcome.refreshed_repositories_with_errors += 1
                        self._log_and_display_paths_with_errors(
                            repository_path=repo.path,
                            repository_refresh_outcome=repository_refresh_outcome,
                            debug=debug,
                            verbose=verbose,
                        )
                    else:
                        refresh_outcome.refreshed_repositories += 1
                        self._log_and_display_repository_refresh_success_message(
                            repository_path=repo.path
                        )

                    self._log_and_display_successful_paths(
                        repository_refresh_outcome=repository_refresh_outcome,
                        verbose=verbose,
                    )
            except Exception as exc:
                # Refresh all repositories that can be refreshed and log
                # failed refreshes.
                refresh_outcome.failed_repositories += 1
                repository_error = str(exc)
                repository_error_tb = ",".join(traceback.format_exception(exc)).replace(
                    "\n", " "
                )

                self._log_and_display_repository_level_refresh_error(
                    repository_path=repo.path, error=exc, debug=debug
                )

            refresh_repositories.append(
                RefreshRepositoryORM(
                    repository_id=repo.id,
                    path=repo.path,
                    total_files=(
                        len(repository_refresh_outcome.failed_paths)
                        + len(repository_refresh_outcome.successful_paths)
                        if repository_error is None
                        else None
                    ),
                    files_refreshed=(
                        len(repository_refresh_outcome.successful_paths)
                        if repository_error is None
                        else None
                    ),
                    files_failed=(
                        len(repository_refresh_outcome.failed_paths)
                        if repository_error is None
                        else None
                    ),
                    error_occured=0 if repository_error is None else 1,
                    error=repository_error,
                    error_tb=repository_error_tb,
                )
            )

        refresh_outcome.total_files = (
            refresh_outcome.failed_files + refresh_outcome.refreshed_files
        )

        duration = round(time.time() - start, 2)
        refresh_outcome.duration = duration

        error_writing_result = self._populate_refresh_database_records(
            global_session=session,
            global_refresh_outcome=refresh_outcome,
            refresh_repositories=refresh_repositories,
            refresh_files_by_repository=refresh_files_by_repository,
        )

        self._log_and_display_global_refresh_summary(
            global_refresh_outcome=refresh_outcome,
            errors_while_getting_file_statistics=errors_while_getting_file_statistics,
            error_writing_result=error_writing_result,
            duration=duration,
            debug=debug,
        )

        return refresh_outcome
