from typing import List, Tuple, Dict
from datetime import datetime
import uuid
import time
import traceback
import sys
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
    def __init__(self, config: Config):
        self.config = config
        self.dir_path, self.db_path = md_utils.get_global_paths(config)

        # Initalize global database if it doesn't exists.
        with GlobalSessionOrExit(db_path=self.db_path):
            pass

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
            if debug:
                print(f"{traceback.format_exception(repos)}\n", file=sys.stderr)

            print("fatal: failed to list repositories", file=sys.stderr)
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

        valid_repos = self._list_repositories_or_exit(session=session)

        errors_while_getting_file_statistics: List[Exception] = (
            []
        )  # TODO: this one should be used once loggin is implemented

        refresh_outcome = GlobalRefreshOutcome.new()
        refresh_outcome.total_repositories = len(valid_repos)
        refresh_repositories: List[RefreshRepositoryORM] = []
        repository_error: str | None = None
        repository_error_tb: str | None = None
        repository_refresh_outcome = LocalRefreshOutcome.new()
        refresh_files_by_repository: Dict[str, List[RefreshFileORM]] = {}

        md_utils.print_centered_message(message=" repositories ", filler_char="=")
        print()

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

                    # General error, not specific to individual record.
                    if repository_refresh_outcome.error is not None:
                        refresh_outcome.failed_repositories += 1

                        if debug:
                            print(
                                f"{RED}{traceback.format_exception(repository_refresh_outcome.error)}\n{RESET}",
                                file=sys.stderr,
                            )

                        print(
                            f"{repo.path} {RED}(failed to refresh){RESET}",
                            file=sys.stderr,
                        )

                    # Record-specific errors.
                    if repository_refresh_outcome.failed_paths:
                        refresh_outcome.refreshed_repositories_with_errors += 1
                        print(f"{repo.path} {YELLOW}(refreshed with errors){RESET}")

                        if verbose or debug:
                            for (
                                path_with_errors
                            ) in repository_refresh_outcome.failed_paths:
                                for error in path_with_errors.errors:
                                    print(
                                        f"  {RED}(failed){RESET}\t{path_with_errors.path} "
                                    )

                                    if debug:
                                        print(
                                            f"\n{traceback.format_exception(error)}\n",
                                            file=sys.stderr,
                                        )
                    else:
                        refresh_outcome.refreshed_repositories += 1
                        print(f"{repo.path} {GREEN}(refreshed){RESET}")

                    if repository_refresh_outcome.successful_paths and verbose:
                        for path in repository_refresh_outcome.successful_paths:
                            print(f"  {GREEN}(successful){RESET}\t{path}")

            except Exception as exc:
                # Refresh all repositories that can be refreshed and log
                # failed refreshes.
                refresh_outcome.failed_repositories += 1
                repository_error = str(exc)
                repository_error_tb = ",".join(traceback.format_exception(exc)).replace(
                    "\n", " "
                )
                print(f"{repo.path} {RED}(failed to refresh){RESET}", file=sys.stderr)
                if debug:
                    print(f"\n{traceback.format_exc()}\n", file=sys.stderr)

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

        # Display outcome.
        print()
        md_utils.print_centered_message(
            message=f" refresh summary (in {duration}s) ", filler_char="="
        )
        print()
        refresh_outcome.pretty_print()

        if error_writing_result:
            print()
            print("error: failed to write refresh records to database")
            if debug:
                print(
                    f"\n{traceback.format_exception(error_writing_result)}",
                    file=sys.stderr,
                )

        return refresh_outcome
