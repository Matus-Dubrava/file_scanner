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
@click.option("--debug", is_flag=True, show_default=True, default=False)
@click.option(
    "--load-from-parent-repository", is_flag=True, show_default=True, default=False
)
@click.option("--recreate", is_flag=True, show_default=True, default=False)
@click.pass_context
def init(ctx, target, debug, load_from_parent_repository, recreate):
    mdm = MetadataManager.new(
        md_config=ctx.obj, path=Path(target).resolve(), recreate=recreate
    )

    if load_from_parent_repository:
        mdm.load_data_from_parent_repository(debug=debug)


@cli.command()
@click.argument("target")
@click.option("--repository-path", required=False)
@click.pass_context
def touch(ctx, target, repository_path) -> None:
    mdm_config = ctx.obj
    source_path = Path.cwd() if not repository_path else Path(repository_path).resolve()

    if not repository_path:
        cli_utils.validate_cwd_is_in_mdm_repository(config=mdm_config)
        cli_utils.validate_cwd_and_target_repository_match(
            config=mdm_config,
            target_path=Path(target).resolve(),
            source_path=source_path,
        )

    mdm = MetadataManager.from_repository(md_config=ctx.obj, path=source_path)
    mdm.touch(Path(target).resolve())


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
@click.option("--repository-path", required=False)
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
        cli_utils.validate_cwd_is_in_mdm_repository(config=mdm_config)

    mdm = MetadataManager.from_repository(
        md_config=ctx.obj,
        path=Path.cwd() if not repository_path else Path(repository_path).resolve(),
    )
    mdm.list_files(
        path=Path.cwd(),
        status_filter=status_filters,
        abs_paths=abs_paths,
        no_header=no_header,
    )


@cli.command()
@click.argument("target")
@click.pass_context
def untrack(ctx, target) -> None:
    mdm_config = ctx.obj
    cli_utils.validate_cwd_is_in_mdm_repository(config=mdm_config)

    mdm = MetadataManager.from_repository(
        md_config=ctx.obj, path=Path(target).resolve()
    )
    mdm.untrack(Path(target).resolve())


@cli.command()
@click.pass_context
def purge(ctx) -> None:
    mdm_config = ctx.obj
    cli_utils.validate_cwd_is_in_mdm_repository(config=mdm_config)

    mdm = MetadataManager.from_repository(md_config=ctx.obj, path=Path.cwd())
    mdm.purge_removed_files(Path.cwd())


@cli.command()
@click.argument("file", nargs=1, required=True)
@click.option("--debug", is_flag=True, show_default=True, default=False)
@click.option("--purge", is_flag=True, show_default=True, default=False)
@click.option("--force", is_flag=True, show_default=True, default=False)
@click.pass_context
def rm(ctx, file, debug, purge, force) -> None:
    mdm_config = ctx.obj
    cli_utils.validate_cwd_is_in_mdm_repository(config=mdm_config)

    mdm = MetadataManager.from_repository(
        md_config=mdm_config, path=Path(file).resolve()
    )
    mdm.remove_file(
        filepath=Path(file).resolve(),
        purge=purge,
        debug=debug,
        force=force,
    )


if __name__ == "__main__":
    mdm_config = Config.from_file(CONFIG_PATH)
    if isinstance(mdm_config, Exception):
        print("Failed to load configuration. Abort.", file=sys.stderr)
        sys.exit(101)

    cli(obj=mdm_config)
