from pathlib import Path
import sys

import click

from manager import MetadataManager
from md_models import Config
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
def touch(ctx, target, repository_path):
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
@click.pass_context
def list(ctx):
    mdm_config = ctx.obj
    cli_utils.validate_cwd_is_in_mdm_repository(config=mdm_config)

    mdm = MetadataManager.from_repository(md_config=ctx.obj, path=Path.cwd())
    mdm.list_files(Path.cwd())


@cli.command()
@click.argument("target")
@click.pass_context
def untrack(ctx, target):
    mdm_config = ctx.obj
    cli_utils.validate_cwd_is_in_mdm_repository(config=mdm_config)

    mdm = MetadataManager.from_repository(
        md_config=ctx.obj, path=Path(target).resolve()
    )
    mdm.untrack(Path(target).resolve())


@cli.command()
@click.pass_context
def purge(ctx):
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
def rm(ctx, file, debug, purge, force):
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
