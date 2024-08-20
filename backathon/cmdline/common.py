import dataclasses

import click

import backathon.db
import backathon.repository


@dataclasses.dataclass
class BackathonContext:
    repo: backathon.repository.Backathon
    db: backathon.db.Database

    @classmethod
    def from_click_context(cls, ctx: click.Context):
        db = backathon.db.Database(ctx.obj["db_path"])
        repo = backathon.repository.Backathon(db)
        return cls(repo=repo, db=db)
