from typing import List
import traceback
import sys
from pathlib import Path

from sqlalchemy.orm import Session

from models.local_models import Config
from models.global_models import RepositoriesORM
import md_utils


class GlobalManager:
    def __init__(self, config: Config):
        self.config = config
        self.dir_path, self.db_path = md_utils.get_global_paths(config)

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
