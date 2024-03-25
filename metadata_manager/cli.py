from pathlib import Path

import click

from manager import MetadataManager
from md_models import Config
import md_utils

CONFIG_PATH = Path(__file__).parent / "config" / ".mdconfig"


@click.group()
@click.pass_context
def cli(ctx):
    pass


@cli.command()
@click.argument("target", default=Path.cwd())
@click.pass_context
def init(ctx, target):
    MetadataManager.new(md_config=ctx.obj, path=Path(target).absolute())


@cli.command()
@click.argument("target")
@click.pass_context
def touch(ctx, target):
    mdm = MetadataManager.from_repository(
        md_config=ctx.obj, path=Path(target).absolute()
    )
    mdm.touch(Path(target).absolute())


@cli.command()
@click.pass_context
def list(ctx):
    mdm = MetadataManager.from_repository(md_config=ctx.obj, path=Path.cwd().absolute())
    mdm.list_files(Path.cwd())


@cli.command()
@click.argument("target")
@click.pass_context
def untrack(ctx, target):
    mdm = MetadataManager.from_repository(
        md_config=ctx.obj, path=Path(target).absolute()
    )
    mdm.untrack(Path(target).absolute())


@cli.command()
@click.pass_context
def purge(ctx):
    mdm = MetadataManager.from_repository(md_config=ctx.obj, path=Path.cwd().absolute())
    mdm.purge_removed_files(Path.cwd().absolute())


@cli.command()
@click.argument("file", nargs=1, required=False)
@click.option("--debug", is_flag=True, show_default=True, default=False)
@click.option("--purge", is_flag=True, show_default=True, default=False)
@click.option("--force", is_flag=True, show_default=True, default=False)
@click.pass_context
def rm(ctx, file, debug, purge, force):
    mdm_config = ctx.obj

    # Validate that the location where the command is issued from is valid
    # Mdm repository.
    md_utils.get_mdm_root_or_exit(path=Path.cwd(), config=mdm_config)

    mdm = MetadataManager.from_repository(
        md_config=mdm_config, path=Path(file).absolute()
    )
    mdm.remove_file(
        filepath=Path(file).absolute(),
        purge=purge,
        debug=debug,
        force=force,
    )


if __name__ == "__main__":
    with open(CONFIG_PATH, "r") as f:
        mdm_config = Config.model_validate_json(f.read())
        cli(obj=mdm_config)
