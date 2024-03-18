from pathlib import Path

import click

from manager import initalize_md_repository


@click.group()
def cli():
    pass


@cli.command()
@click.argument("target", default=Path.cwd())
@click.option(
    "--yes", "-y", is_flag=True, default=False, help="Initialize without confirmation"
)
def init(target, yes):
    initalize_md_repository(Path(target), force=yes)


if __name__ == "__main__":
    cli()
