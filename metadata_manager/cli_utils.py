from pathlib import Path
import sys

from md_utils import get_mdm_root_or_exit, get_mdm_root
from md_models import Config
import md_constants


def validate_cwd_is_in_mdm_repository(config: Config):
    get_mdm_root_or_exit(path=Path.cwd(), config=config)


def validate_cwd_and_target_repository_match(
    config: Config, target_path: Path, source_path: Path = Path.cwd()
):
    """
    Block command when it is unclear which repository the command should be attributed to.
    This situation can happen when there are subrepositories and there is a mismatch
    between the Mdm repository where the command was run from and the Mdm repository
    where the target path points to.

    This applies to commands such as 'mdm touch'.

    ex:
    (repository 1) /dir1
    (repository 2) /dir1/dir2

    command is run from /dir1
        - mdm touch /dir1/dir2/somefile

    Sourve repository is repository 1, while the path provided to 'mdm touch' points to repository 2.
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
