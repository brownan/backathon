import logging
import os
import pathlib
import subprocess
import sys
import time

import click
import uvicorn

logger = logging.getLogger("backathon.server")


@click.command()
@click.pass_context
def dev(ctx: click.Context):
    db_path = ctx.obj["db_path"]
    os.environ.setdefault("BACKATHON_DB_PATH", str(db_path))
    nvm_bin = os.environ.get("NVM_BIN", "")
    npx_path = pathlib.Path(nvm_bin, "npx")

    newenv = dict(os.environ)
    if nvm_bin:
        newenv["PATH"] = newenv["PATH"] + ":" + nvm_bin

    logger.info("Starting vite server")
    vite = subprocess.Popen(
        [str(npx_path), "vite", "--clearScreen", "false"],
        cwd="backathon-web",
        stdin=subprocess.DEVNULL,
        env=newenv,
    )
    time.sleep(1)
    if vite.poll() is not None:
        logger.error("Vite server did not start correctly")
        sys.exit(1)
    try:
        uvicorn.run(
            "backathon.api:dev_app",
            port=8000,
            log_level="info",
            reload=True,
        )
    finally:
        vite.terminate()
        try:
            vite.wait(10)
        except subprocess.TimeoutExpired:
            vite.kill()
            logger.warning("Vite process killed")
        else:
            logger.info("Vite server shutdown")
