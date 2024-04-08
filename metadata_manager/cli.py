from pathlib import Path
import sys
from typing import List, Set

import click

from manager import MetadataManager
from md_models import Config
from md_enums import FileStatus
import md_constants
import cli_utils
from db import get_session_or_exit
import md_utils

CONFIG_PATH = Path(__file__).parent / "config" / ".mdconfig"


@click.group()
@click.pass_context
def cli(ctx):
    pass


@cli.command()
@click.argument("target", default=Path.cwd())
@click.option(
    "--debug",
    is_flag=True,
    show_default=True,
    default=False,
    help="Print debug information.",
)
@click.option(
    "--load-from-parent-repository", is_flag=True, show_default=True, default=False
)
@click.option("--recreate", is_flag=True, show_default=True, default=False)
@click.pass_context
def init(ctx, target, debug, load_from_parent_repository, recreate):
    mdm = MetadataManager.new(
        md_config=ctx.obj, path=Path(target).resolve(), recreate=recreate, debug=debug
    )

    if load_from_parent_repository:
        mdm.load_data_from_parent_repository(debug=debug)


@cli.command()
@click.argument("path", nargs=-1, required=True)
@click.option(
    "--repository-path",
    required=False,
    help=(
        "Path to repository. If path doesn't point to repository root the nearest parent repository is used. "
        "Fails if no parent repository is found."
    ),
)
@click.option(
    "-p",
    "--parents",
    is_flag=True,
    default=False,
    help="Create parent directories as needed. No error when parent directories exist.",
)
@click.option(
    "--debug",
    is_flag=True,
    show_default=True,
    default=False,
    help="Print debug information.",
)
@click.pass_context
def touch(ctx, path, repository_path, parents, debug) -> None:
    mdm_config = ctx.obj
    source_path = Path.cwd() if not repository_path else Path(repository_path).resolve()
    target_paths = [Path(p).resolve() for p in path]
    mdm = MetadataManager.from_repository(md_config=ctx.obj, path=source_path)

    cli_utils.validate_paths(
        with_repository_path=repository_path,
        config=mdm_config,
        mdm=mdm,
        target_paths=target_paths,
        debug=debug,
    )

    # Validate all parent directories exists if 'parents' flag wasn't provided.
    if not parents:
        for target_path in target_paths:
            if not target_path.parent.exists():
                print(
                    f"fatal: directory {target_path.parent} doesn't exist",
                    file=sys.stderr,
                )
                print(
                    "\nprovide -p/--parents flag to create parent directories as needed.",
                    file=sys.stderr,
                )
                sys.exit(1)

    # Validate there are no directories in provided paths.
    # Touching existing directory is not allowed.
    for path in target_paths:
        if path.is_dir():
            print(f"fatal: can't touch directory {path}", file=sys.stderr)
            sys.exit(2)

    session = get_session_or_exit(db_path=mdm.db_path, debug=debug)
    for target_path in target_paths:
        mdm.touch(session=session, filepath=target_path, create_parents=parents)

    session.close()


@cli.command()
@click.option("--all", is_flag=True, show_default=True, default=False)
@click.option("--active", is_flag=True, show_default=True, default=False)
@click.option("--removed", is_flag=True, show_default=True, default=False)
@click.option("--untracked", is_flag=True, show_default=True, default=False)
@click.option("--subrepository-tracked", is_flag=True, show_default=True, default=False)
@click.option(
    "--abs-paths",
    is_flag=True,
    show_default=True,
    default=False,
    help="List absolute file paths.",
)
@click.option(
    "--no-header",
    is_flag=True,
    show_default=True,
    default=False,
    help="List only files without printing any other information such as repository id, repository path or active filters",
)
@click.option(
    "--dump-json",
    required=False,
    help=(
        "Dump the result into provided filepath in JSON format. "
        "Filters can still be applied. This operation fails if parent directory doesn't exist. "
        "Use --force/-f flag to create parent directories."
    ),
)
@click.option("--repository-path", required=False)
@click.option(
    "--debug", is_flag=True, show_default=True, default=False, help="Print debug info."
)
@click.option(
    "--force",
    "-f",
    is_flag=True,
    show_default=True,
    default=False,
    help="Use together with --dump-json option to create parent directories if missing.",
)
@click.pass_context
def ls(
    ctx,
    all,
    active,
    removed,
    untracked,
    subrepository_tracked,
    repository_path,
    abs_paths,
    no_header,
    dump_json,
    debug,
    force,
) -> None:
    status_filters: List[FileStatus] = []

    if all:
        status_filters = [
            FileStatus.ACTIVE,
            FileStatus.REMOVED,
            FileStatus.UNTRACKED,
            FileStatus.TRACKED_IN_SUBREPOSITORY,
        ]
    else:
        if active:
            status_filters.append(FileStatus.ACTIVE)
        if removed:
            status_filters.append(FileStatus.REMOVED)
        if untracked:
            status_filters.append(FileStatus.UNTRACKED)
        if subrepository_tracked:
            status_filters.append(FileStatus.TRACKED_IN_SUBREPOSITORY)

    # Show only "ACTIVE" files by default.
    if not status_filters:
        status_filters.append(FileStatus.ACTIVE)

    mdm_config = ctx.obj

    if not repository_path:
        cli_utils.validate_cwd_is_within_repository_dir(config=mdm_config)

    mdm = MetadataManager.from_repository(
        md_config=ctx.obj,
        path=Path.cwd() if not repository_path else Path(repository_path).resolve(),
    )

    session = get_session_or_exit(db_path=mdm.db_path)
    mdm.list_files(
        session=session,
        path=Path.cwd(),
        status_filter=status_filters,
        abs_paths=abs_paths,
        no_header=no_header,
        dump_json_path=Path(dump_json).resolve() if dump_json else None,
        debug=debug,
        force=force,
    )
    session.close()


@cli.command()
@click.argument("target")
@click.pass_context
def untrack(ctx, target) -> None:
    mdm_config = ctx.obj
    cli_utils.validate_cwd_is_within_repository_dir(config=mdm_config)

    mdm = MetadataManager.from_repository(
        md_config=ctx.obj, path=Path(target).resolve()
    )
    session = get_session_or_exit(mdm.db_path)

    mdm.untrack(session=session, filepath=Path(target).resolve())
    session.close()


@cli.command()
@click.pass_context
@click.option("--debug", is_flag=True, show_default=True, default=False)
def purge(ctx, debug) -> None:
    mdm_config = ctx.obj
    cli_utils.validate_cwd_is_within_repository_dir(config=mdm_config)

    mdm = MetadataManager.from_repository(md_config=ctx.obj, path=Path.cwd())
    session = get_session_or_exit(db_path=mdm.db_path)

    mdm.purge_removed_files(session=session, path=Path.cwd(), debug=debug)
    session.close()


@cli.command()
@click.argument("path", nargs=-1, required=True)
@click.option("--debug", is_flag=True, show_default=True, default=False)
@click.option("--purge", is_flag=True, show_default=True, default=False)
@click.option("--force", is_flag=True, show_default=True, default=False)
@click.option(
    "--keep-local",
    is_flag=True,
    show_default=True,
    default=False,
    help="Remove the files/directories from internal database but preserve them in file system.",
)
@click.option(
    "--repository-path",
    required=False,
    help=(
        "Path to repository. If path doesn't point to repository root the nearest parent repository is used. "
        "Fails if no parent repository is found."
    ),
)
@click.option(
    "-r",
    "--recursive",
    is_flag=True,
    show_default=True,
    default=False,
    help="Required if any of the provided paths is directory.",
)
@click.pass_context
def rm(ctx, path, debug, purge, force, repository_path, recursive, keep_local) -> None:
    mdm_config = ctx.obj

    source_path = Path.cwd() if not repository_path else Path(repository_path).resolve()
    target_paths = [Path(p).resolve() for p in path]
    mdm = MetadataManager.from_repository(md_config=mdm_config, path=source_path)

    cli_utils.validate_paths(
        with_repository_path=repository_path,
        config=mdm_config,
        mdm=mdm,
        debug=debug,
        target_paths=target_paths,
    )

    session = get_session_or_exit(db_path=mdm.db_path)
    target_filepaths: List[Path] = []
    tracked_dirs: Set[str] = set()
    # Exit if any of provided paths is directory and --recursive flag is not set,
    # otherwise collect all tracked paths and subdirectories, including the 'root' directory
    # so that those can be deleted.
    for path in target_paths:
        # In case specified path doesn't exists, find all files that might be associated with this
        # path in the database and add them for removal. This ensure that dangling
        # objects are removed.
        if not path.exists():
            target_filepaths.extend(
                md_utils.find_tracked_files_in_database(session=session, path=path)
            )
        elif path.is_dir():
            if not recursive:
                print(
                    f"fatal: can't remove directory {path} without -r/--recursive flag",
                    file=sys.stderr,
                )
                sys.exit(md_constants.MISSING_RECURSIVE_FLAG)
            else:
                files, dirs = md_utils.get_tracked_files_and_subdirectories(
                    session=session, path=path
                )
                # If there are no files to be removed from a provided directory, inform
                # user about this fact to avoid confusion when the 'rm' operation is successful
                # but has no effect.
                if not files:
                    print(f"skip: {path}, no tracked files found")
                else:
                    target_filepaths.extend(files)
                    tracked_dirs = tracked_dirs.union(dirs)
        else:
            target_filepaths.append(path)

    mdm.remove_files(
        session=session,
        filepaths=target_filepaths,
        purge=purge,
        debug=debug,
        force=force,
        keep_local=keep_local,
    )
    session.close()

    # Remove empty directories.
    # Traverse tracked directories from leaves to the root, removing each empty
    # directory along the way.
    # Note that only directories that were tracked are removed. Empty directories that did not
    # contain any tracked files are kept in place.
    for dir_ in sorted(
        [Path(path) for path in tracked_dirs],
        key=lambda path: len(Path(path).parts),
        reverse=True,
    ):
        # Find empty directories and remove them unless --keep local is provided.
        if not list(dir_.iterdir()) and not keep_local:
            dir_.rmdir()

        # Find empty hash directories and remove them.
        if not list(mdm.get_path_to_hash_file(filepath=dir_).iterdir()):
            mdm.remove_hash_file_or_dir(path=dir_)


@cli.command()
@click.argument("paths", nargs=-1, required=True)
@click.option(
    "--debug",
    is_flag=True,
    show_default=True,
    default=False,
    help="Show debug information.",
)
@click.option(
    "--repository-path",
    required=False,
    help=(
        "Path to repository. If path doesn't point to repository root the nearest parent repository is used. "
        "Fails if no parent repository is found."
    ),
)
@click.pass_context
def add(ctx, paths, debug, repository_path):
    mdm_config = ctx.obj

    source_path = Path.cwd() if not repository_path else Path(repository_path).resolve()
    target_paths = [Path(p).resolve() for p in paths]
    mdm = MetadataManager.from_repository(md_config=mdm_config, path=source_path)

    cli_utils.validate_paths(
        with_repository_path=repository_path,
        config=mdm_config,
        mdm=mdm,
        debug=debug,
        target_paths=target_paths,
    )

    for target_path in target_paths:
        # Can't add nonexistent files.
        if not target_path.exists():
            print(f"fatal: {target_path} doesn't exist", file=sys.stderr)
            sys.exit(1)

    session = get_session_or_exit(db_path=mdm.db_path)

    for target_path in target_paths:
        if target_path.is_file():
            mdm.add_file(session=session, filepath=target_path, debug=debug)
        elif target_path.is_dir():
            mdm.add_directory(session=session, dirpath=target_path, debug=debug)

    session.close()


@cli.command()
@click.option("--repository-path", required=False)
@click.option(
    "--debug",
    is_flag=True,
    show_default=True,
    default=False,
    help="Show debug information.",
)
@click.option("--verbose", "-v", is_flag=True, show_default=True, default=False)
@click.pass_context
def refresh(ctx, repository_path, debug, verbose):
    mdm_config = ctx.obj

    source_path = Path.cwd() if not repository_path else Path(repository_path).resolve()
    mdm = MetadataManager.from_repository(
        md_config=mdm_config, path=source_path, debug=debug
    )

    session = get_session_or_exit(db_path=mdm.db_path, debug=debug)
    mdm.refresh_active_repository_records(session=session, debug=debug, verbose=verbose)
    session.close()


if __name__ == "__main__":
    mdm_config = Config.from_file(CONFIG_PATH)
    if isinstance(mdm_config, Exception):
        print("Failed to load configuration. Abort.", file=sys.stderr)
        sys.exit(md_constants.CANT_LOAD_CONFIGURATION)

    cli(obj=mdm_config)
