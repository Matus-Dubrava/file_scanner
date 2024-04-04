from pathlib import Path
import traceback
import sys
from typing import List

from md_utils import get_repository_root_or_exit
from md_models import Config
import md_constants
from manager import MetadataManager


def validate_cwd_is_within_repository_dir(config: Config) -> None:
    get_repository_root_or_exit(path=Path.cwd(), config=config)


def validate_source_and_target_repository_dirs_match(
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
    target_repository_root = get_repository_root_or_exit(
        path=target_path.parent, config=config
    )
    cwd_mdm_root = get_repository_root_or_exit(path=source_path, config=config)

    if str(target_repository_root) != str(cwd_mdm_root):
        print("Fatal: ambiguous repository.", file=sys.stderr)
        print(
            "\nCurrent repository doesn't match target repository.",
            file=sys.stderr,
        )
        print(f"\tcurrent repository: {cwd_mdm_root}", file=sys.stderr)
        print(f"\ttarget repository: {target_path.parent}", file=sys.stderr)
        print(
            "\nTo resolve this issue, run the command from target repository\nor provide --repository-path option",
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


def validate_paths(
    with_repository_path: bool,
    config: Config,
    mdm: MetadataManager,
    target_paths: List[Path],
    debug: bool = False,
):
    """
    Perform validation checks depending on whether repository path was explicitely provided or not.
    For a path to be considered valid, it needs to be within repository. Futhermore source repository
    must match target repository unless repository path is explictly provided.

    Exit if validation fails.

    with_repository_path:   Flag indicating whether repository path was explicitely provided. CWD is the
                            default path for CLI operations.
    config:                 Manager config.
    mdm:                    Manager object.
    target_paths:           List of paths to perform operations at (such as 'touch' or 'rm').
    debug:                  Show debug information.
    """

    # Repository path wasn't directly provided, pefrom validation against CWD.
    if not with_repository_path:
        validate_cwd_is_within_repository_dir(config=config)
        for path in target_paths:
            # Check if every paths fall under the same repositry as CWD.
            # This is necessary because CWD may not be in the same repositry
            # as the location where the provided paths point to in which case
            # the validation fails.
            validate_source_and_target_repository_dirs_match(
                config=config,
                target_path=path,
                source_path=Path.cwd(),
            )

    # Repository path was provided. Check if each path in 'target_paths' is within that directory.
    if with_repository_path:
        for path in target_paths:
            validate_path_is_within_repository_dir(mdm=mdm, path=path, debug=debug)
