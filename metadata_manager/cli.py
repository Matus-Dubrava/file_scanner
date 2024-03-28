from pathlib import Path
import sys
from typing import List

import click

from manager import MetadataManager
from md_models import Config
from md_enums import FileStatus
import cli_utils

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
@click.argument("target")
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
def touch(ctx, target, repository_path, parents, debug) -> None:
    mdm_config = ctx.obj
    source_path = Path.cwd() if not repository_path else Path(repository_path).resolve()
    target_path = Path(target).resolve()

    if not repository_path:
        cli_utils.validate_cwd_is_within_repository_dir(config=mdm_config)
        cli_utils.validate_cwd_and_target_repository_dirs_match(
            config=mdm_config,
            target_path=target_path,
            source_path=source_path,
        )

    mdm = MetadataManager.from_repository(md_config=ctx.obj, path=source_path)
    if repository_path:
        cli_utils.validate_path_is_within_repository_dir(
            mdm=mdm, path=target_path, debug=debug
        )

    mdm.touch(filepath=target_path, parents=parents)


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
    mdm.list_files(
        path=Path.cwd(),
        status_filter=status_filters,
        abs_paths=abs_paths,
        no_header=no_header,
        dump_json_path=Path(dump_json).resolve() if dump_json else None,
        debug=debug,
        force=force,
    )


@cli.command()
@click.argument("target")
@click.pass_context
def untrack(ctx, target) -> None:
    mdm_config = ctx.obj
    cli_utils.validate_cwd_is_within_repository_dir(config=mdm_config)

    mdm = MetadataManager.from_repository(
        md_config=ctx.obj, path=Path(target).resolve()
    )
    mdm.untrack(Path(target).resolve())


@cli.command()
@click.pass_context
def purge(ctx) -> None:
    mdm_config = ctx.obj
    cli_utils.validate_cwd_is_within_repository_dir(config=mdm_config)

    mdm = MetadataManager.from_repository(md_config=ctx.obj, path=Path.cwd())
    mdm.purge_removed_files(Path.cwd())


@cli.command()
@click.argument("path", nargs=-1, required=True)
@click.option("--debug", is_flag=True, show_default=True, default=False)
@click.option("--purge", is_flag=True, show_default=True, default=False)
@click.option("--force", is_flag=True, show_default=True, default=False)
@click.option(
    "--repository-path",
    required=False,
    help=(
        "Path to repository. If path doesn't point to repository root the nearest parent repository is used. "
        "Fails if no parent repository is found."
    ),
)
@click.pass_context
def rm(ctx, path, debug, purge, force, repository_path) -> None:
    mdm_config = ctx.obj

    source_path = Path.cwd() if not repository_path else Path(repository_path).resolve()
    target_paths = [Path(p).resolve() for p in path]
    mdm = MetadataManager.from_repository(md_config=mdm_config, path=source_path)

    if not repository_path:
        cli_utils.validate_cwd_is_within_repository_dir(config=mdm_config)
        for path in target_paths:
            cli_utils.validate_cwd_and_target_repository_dirs_match(
                config=mdm_config,
                target_path=path,
                source_path=source_path,
            )

    if repository_path:
        for path in target_paths:
            cli_utils.validate_path_is_within_repository_dir(
                mdm=mdm, path=path, debug=debug
            )

    mdm.remove_files(filepaths=target_paths, purge=purge, debug=debug, force=force)


if __name__ == "__main__":
    mdm_config = Config.from_file(CONFIG_PATH)
    if isinstance(mdm_config, Exception):
        print("Failed to load configuration. Abort.", file=sys.stderr)
        sys.exit(101)

    cli(obj=mdm_config)
