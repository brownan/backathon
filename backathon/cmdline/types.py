import pathlib
from typing import Annotated

import click
import typer

PathType = click.Path(dir_okay=False, readable=True, path_type=pathlib.Path)
PathOption = Annotated[pathlib.Path, typer.Argument(click_type=PathType)]
