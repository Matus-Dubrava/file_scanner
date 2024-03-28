from pathlib import Path
import traceback
import sys

from md_utils import get_mdm_root_or_exit, get_mdm_root
from md_models import Config
import md_constants
from manager import MetadataManager


def validate_cwd_is_within_repository_dir(config: Config) -> None:
    get_mdm_root_or_exit(path=Path.cwd(), config=config)


def validate_cwd_and_target_repository_dirs_match(
    config: Config, target_path: Path, source_path: Path = Path.cwd()
) -> None:
    """
    Block command when it is unclear which repository the command should be attributed to.
    This situation can happen when there are subrepositories and there is a mismatch
    between the repository where the command was run from and the repository
    where the target path points to.

    This applies to commands such as 'touch', 'rm' etc.

    ex:
    (repository 1) /dir1
    (repository 2) /dir1/dir2

    command is run from /dir1
        - touch /dir1/dir2/somefile

    Source repository is repository 1, while the path provided to 'touch' points to repository 2.
    In such situation, it is unclear which repository the command should operate on.
    """
    target_mdm_root = get_mdm_root(path=target_path.parent, config=config)
    cwd_mdm_root = get_mdm_root(path=source_path, config=config)

    if str(target_mdm_root) != str(cwd_mdm_root):
        print("Fatal: ambiguous repository.", file=sys.stderr)
        print(
            "\nCurrent Mdm repository doesn't match target Mdm repository.",
            file=sys.stderr,
        )
        print(f"\tcurrent repository: {cwd_mdm_root}", file=sys.stderr)
        print(f"\ttarget repository: {target_path.parent}", file=sys.stderr)
        print(
            "\nTo resolve this issue, run the command from target Mdm repository\nor provide --repository-path option",
            file=sys.stderr,
        )
        sys.exit(md_constants.AMBIGUOUS_REPOSITORY)


def validate_path_is_within_repository_dir(
    mdm: MetadataManager, path: Path, debug: bool = False
) -> None:
    try:
        path.relative_to(mdm.repository_root)
    except Exception:
        if debug:
            print(f"{traceback.format_exc()}\n", file=sys.stderr)

        print(
            "Fatal: provided path is in not within repository.\n",
            f"provided path:\t\t{path}\n",
            f"repository_path:\t{mdm.repository_root}",
            file=sys.stderr,
        )
        sys.exit(md_constants.PATH_NOT_WITHIN_REPOSITORY)
