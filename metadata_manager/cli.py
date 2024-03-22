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
@click.option(
    "--yes", "-y", is_flag=True, default=False, help="Initialize without confirmation"
)
@click.pass_context
def init(ctx, target, yes):
    md_manager: MetadataManager = ctx.obj
    md_manager.initalize_md_repository(Path(target).absolute(), force=yes)


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


if __name__ == "__main__":
    with open(CONFIG_PATH, "r") as f:
        md_config = Config.model_validate_json(f.read())

    md_manager = MetadataManager(md_config)
    cli(obj=md_manager)
