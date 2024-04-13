from typing import List
import traceback
import sys
from pathlib import Path

from sqlalchemy.orm import Session

from models.local_models import Config, GlobalRefreshOutcome
from models.global_models import RepositoriesORM
import md_utils
from manager import MetadataManager
from db import LocalSession, GlobalSessionOrExit


class GlobalManager:
    def __init__(self, config: Config):
        self.config = config
        self.dir_path, self.db_path = md_utils.get_global_paths(config)

        # Initalize global database if it doesn't exists.
        with GlobalSessionOrExit(db_path=self.db_path):
            pass

    def _list_repositories(
        self, session: Session, all_: bool = False
    ) -> List[RepositoriesORM] | Exception:
        """
        Returns list of repositories.

        all_:   Return only repositories that exists in FS. If repository is deleted,
                its record in database is not automatically removed.
        """
        try:
            repos = session.query(RepositoriesORM).all()

            if all_:
                return repos
            else:
                return [repo for repo in repos if self._is_valid(Path(repo.path))]
        except Exception as exc:
            return exc

    def _is_valid(self, path: Path) -> bool:
        """
        Checks if repository exists at given path.

        path:   Repository path.
        """

        return path.joinpath(self.config.local_dir_name).exists()

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
        repos = self._list_repositories(session=session, all_=all_)

        if isinstance(repos, Exception):
            if debug:
                print(f"{traceback.format_exception(repos)}\n", file=sys.stderr)

            print("fatal: failed to list repositories", file=sys.stderr)
            sys.exit(1)

        for repo in repos:
            print(f"[{repo.id}] {repo.path}")

    def refresh_all_repositories(
        self, session: Session, debug: bool = False, verbose: bool = False
    ) -> GlobalRefreshOutcome:
        """
        Refresh all records in all valid repositories.

        session:    Global database session.
        debug:      Display debug information.
        verbose:    Display path of every file that is being refreshed.
        """

        valid_repos = self._list_repositories(session=session)
        if isinstance(valid_repos, Exception):
            if debug:
                print(f"{traceback.format_exception(valid_repos)}\n", file=sys.stderr)

            print("fatal: failed to get list of repositories", file=sys.stderr)
            sys.exit(1)

        refresh_outcome = GlobalRefreshOutcome.new()
        refresh_outcome.total_repositories = len(valid_repos)

        for repo in valid_repos:
            try:
                mdm = MetadataManager.from_repository(
                    md_config=self.config, path=Path(repo.path), debug=debug
                )

                with LocalSession(db_path=mdm.db_path) as local_session:
                    outcome = mdm._refresh_active_repository_records(
                        session=local_session
                    )
                    refresh_outcome.failed_files += len(outcome.failed_paths)
                    refresh_outcome.refreshed_files += len(outcome.successful_paths)

                    # General error, not specific to individual record.
                    if outcome.error is not None:
                        refresh_outcome.failed_repositories += 1

                        if debug:
                            print(
                                f"{traceback.format_exception(outcome.error)}\n",
                                file=sys.stderr,
                            )

                        print(f"failed to refresh: {repo.path}", file=sys.stderr)

                    # Record-specific errors.
                    if outcome.failed_paths:
                        refresh_outcome.refreshed_repositories_with_errors += 1
                        print(f"refreshed with errors: {repo.path}")

                        if verbose or debug:
                            for path_with_errors in outcome.failed_paths:
                                for error in path_with_errors.errors:
                                    print(
                                        f"\tfailed to refresh file: {path_with_errors.path}"
                                    )

                                    if debug:
                                        print(
                                            f"\terror: {traceback.format_exception(error)}\n",
                                            file=sys.stderr,
                                        )
                    else:
                        refresh_outcome.refreshed_repositories += 1
                        print(f"refreshed: {repo.path}")

                    if outcome.successful_paths and verbose:
                        for path in outcome.successful_paths:
                            print(f"\trefreshed file: {path}")

            except Exception:
                refresh_outcome.failed_repositories += 1
                # Refresh all repositories that can be refreshed and log
                # failed refreshes.
                if debug:
                    print(f"{traceback.format_exc()}\n", file=sys.stderr)

                print(f"refresh failed {repo.path}")

        refresh_outcome.total_files = (
            refresh_outcome.failed_files + refresh_outcome.refreshed_files
        )
        print()
        print("======== REFRESH SUMMARY ========")
        print()
        refresh_outcome.pretty_print()

        return refresh_outcome
