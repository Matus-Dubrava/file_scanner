from pathlib import Path

import click

from manager import MetadataManager
from md_models import Config

CONFIG_PATH = Path(__file__).parent / "config" / ".mdconfig"


@click.group()
@click.pass_context
def cli(ctx):
    pass


@cli.command()
@click.argument("target", default=Path.cwd())
@click.pass_context
def init(ctx, target):
    md_manager: MetadataManager = ctx.obj
    md_manager.initalize_md_repository(Path(target).absolute())


@cli.command()
@click.argument("target")
@click.pass_context
def touch(ctx, target):
    md_manager: MetadataManager = ctx.obj
    md_manager.touch(Path(target).absolute())


@cli.command()
@click.pass_context
def list(ctx):
    md_manager: MetadataManager = ctx.obj
    md_manager.list_files(Path.cwd())


@cli.command()
@click.argument("target")
@click.pass_context
def untrack(ctx, target):
    md_manager: MetadataManager = ctx.obj
    md_manager.untrack(Path(target).absolute())


@cli.command()
@click.pass_context
def purge(ctx):
    mdm: MetadataManager = ctx.obj
    mdm.purge_removed_files(Path.cwd().absolute())


@cli.command()
@click.argument("file", nargs=-1, required=False)
@click.option("--debug", is_flag=True, show_default=True, default=False)
@click.option("--purge", is_flag=True, show_default=True, default=False)
@click.option("--force", is_flag=True, show_default=True, default=False)
@click.pass_context
def rm(ctx, file, debug, purge, force):
    md_manager: MetadataManager = ctx.obj
    if file:
        md_manager.remove_file(
            Path(file[0]).absolute(), purge=purge, debug=debug, force=force
        )
    elif purge:
        md_manager.purge_removed_files(Path.cwd().absolute())
    else:
        click.echo(click.get_current_context().get_help())


if __name__ == "__main__":
    with open(CONFIG_PATH, "r") as f:
        md_config = Config.model_validate_json(f.read())

    md_manager = MetadataManager(md_config)
    cli(obj=md_manager)
