import logging
import os
import pathlib
import sqlite3
import sys

import click
from rich.logging import RichHandler

import backathon.cmdline.backup
import backathon.cmdline.scan
import backathon.cmdline.server
import backathon.db
import backathon.repository
from backathon.cmdline.common import BackathonContext
from backathon.encryption.nacl import NaclEncrypter
from backathon.encryption.null import NullConfig, NullEncrypter
from backathon.storage.local import LocalStorage, LocalStorageConfig

logger = logging.getLogger("backathon.cmdline")


@click.group()
@click.argument(
    "configfile", type=click.Path(dir_okay=False, readable=True, path_type=pathlib.Path)
)
@click.option("--verbose", "-v", is_flag=True)
@click.option("--profile", is_flag=True)
@click.pass_context
def main(
    ctx: click.Context,
    configfile: pathlib.Path,
    verbose: bool,
    profile: bool,
):
    loglevel = logging.INFO if not verbose else logging.DEBUG
    logging.basicConfig(
        format="%(message)s", level=logging.WARNING, handlers=[RichHandler()]
    )
    logging.getLogger("backathon").setLevel(loglevel)

    ctx.ensure_object(dict)
    ctx.obj["db_path"] = configfile

    if profile:
        import atexit
        import cProfile

        logger.info("Profiling enabled")

        p = cProfile.Profile()

        def onexit():
            p.disable()
            p.dump_stats("backathon.pstats")
            print("Profile data dumped to backathon.pstats")

        atexit.register(onexit)
        p.enable()


main.add_command(backathon.cmdline.server.dev)
main.add_command(backathon.cmdline.scan.scan)
main.add_command(backathon.cmdline.backup.backup)


@main.command()
@click.option("--enable-encryption/--disable-encryption", default=True)
@click.argument("destination")
@click.pass_context
def initialize(ctx: click.Context, enable_encryption: bool, destination: str):
    db_path = ctx.obj["db_path"]
    enc_password = os.environ.get("BACKATHON_PASSWORD")
    if enc_password is None and enable_encryption:
        click.echo("Create a password used to encrypt your backup repository")
        click.echo("The password is REQUIRED to decrypt backed-up files")
        click.echo("There is NO WAY to recover backed up files without the password!")
        enc_password = click.prompt(
            "Encryption Password", hide_input=True, confirmation_prompt=True
        )

    storage = LocalStorage(LocalStorageConfig(base_path=pathlib.Path(destination)))
    if enc_password is None:
        encryption = NullEncrypter(NullConfig())
    else:
        click.echo("Generating encryption keys...")
        encryption = NaclEncrypter.new(enc_password)

    backathon.repository.Backathon.initialize(db_path, storage, encryption)
    cmdline_prefix = sys.argv[0]
    click.echo("Config database initialized. Add some roots with")
    click.echo("> {} edit-roots".format(cmdline_prefix))
    click.echo("then run a scan with")
    click.echo("> {} scan".format(cmdline_prefix))
    click.echo("then a backup with")
    click.echo("> {} backup".format(cmdline_prefix))


@main.command()
@click.argument("path", type=click.Path(path_type=pathlib.Path))
@click.pass_context
def add_root(ctx: click.Context, path: pathlib.Path):
    b = BackathonContext.from_click_context(ctx)
    click.echo("Adding root {}".format(path))
    try:
        b.repo.add_root(path)
    except sqlite3.IntegrityError:
        raise click.BadParameter(f"Root {path} already exists", param_hint="path")


@main.command()
@click.pass_context
def list_roots(ctx: click.Context):
    b = BackathonContext.from_click_context(ctx)
    repo = b.repo
    roots = repo.get_roots()
    if roots:
        for root in roots:
            click.echo(root)
    else:
        click.echo("no roots", err=True)


@main.command()
@click.pass_context
def edit_roots(ctx: click.Context):
    b = BackathonContext.from_click_context(ctx)
    repo = b.repo
    roots = repo.get_roots()
    text = "\n".join(str(r.decoded_path) for r in roots)
    new_text = click.edit(text)
    if new_text is not None:
        old_roots_set = set(str(r.decoded_path) for r in roots)
        new_roots = [line.strip() for line in new_text.split("\n")]
        new_roots = [line for line in new_roots if line]
        roots_deleted = 0
        roots_added = 0
        for to_del in set(old_roots_set).difference(new_roots):
            repo.del_root(pathlib.Path(to_del))
            roots_deleted += 1
        for to_add in set(new_roots).difference(old_roots_set):
            repo.add_root(pathlib.Path(to_add))
            roots_added += 1
        if roots_added:
            click.echo(
                "{} root{} added".format(roots_added, "s" if roots_added != 1 else "")
            )
        if roots_deleted:
            click.echo(
                "{} root{} removed".format(
                    roots_deleted, "s" if roots_deleted != 1 else ""
                )
            )
        if roots_deleted:
            # Deleting a root can cascade to a lot of metadata in the fsentry table. Recover
            # a bit of space if there are any empty pages
            click.echo("Cleaning up database...")
            with repo.db.cursor() as cursor:
                cursor.execute("PRAGMA incremental_vacuum")
                cursor.fetchall()

    else:
        click.echo("Roots unmodified")


@main.command()
@click.pass_context
def edit_excludes(ctx: click.Context):
    b = BackathonContext.from_click_context(ctx)
    db = b.db
    current = db.config_get_json("excludes", [])
    text = """# Add excludes, one per line. Globs are supported.\n\n"""
    text += "\n".join(current)
    new_text = click.edit(text)
    if new_text is not None:
        new_lines = [line.strip() for line in new_text.split("\n")]
        new_lines = [line for line in new_lines if line and not line.startswith("#")]
        # TODO: if any excludes were removed, mark all directory entries as new so that
        # they are forced rescanned next scan
        # TODO: if excludes were added, we can do a single pass over the fsentry table
        # to remove any entries that are now excluded
        db.config_set_json("excludes", new_lines)
        click.echo("Exclude list updated")
    else:
        click.echo("Exclude list unchanged")


@main.command()
@click.argument("path", type=click.Path(path_type=pathlib.Path))
@click.pass_context
def set_local_target(ctx: click.Context, path: pathlib.Path):
    b = BackathonContext.from_click_context(ctx)
    repo = b.repo
    repo.db.config_set_json("local_storage_config", {"base_path": str(path.absolute())})


if __name__ == "__main__":
    main()
