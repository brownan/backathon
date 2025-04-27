import logging
import os
import pathlib
import subprocess
import sys
import time

import typer
import uvicorn.config

import backathon.cmdline.main
from backathon.cmdline.types import PathOption

logger = logging.getLogger("backathon.server")

app = typer.Typer()


@app.command()
def run(db_path: PathOption):
    os.environ.setdefault("BACKATHON_DB_PATH", str(db_path))

    from backathon.api import prod_app

    uvicorn.run(
        prod_app,
        http="h11",
        port=8000,
        log_level="info",
        log_config=backathon.cmdline.main.LOGGING_CONFIG,
        reload=False,
    )


@app.command()
def dev(db_path: PathOption):
    os.environ.setdefault("BACKATHON_DB_PATH", str(db_path))
    nvm_bin = os.environ.get("NVM_BIN", "")
    npx_path = pathlib.Path(nvm_bin, "npx")

    newenv = dict(os.environ)
    if nvm_bin:
        newenv["PATH"] = newenv["PATH"] + ":" + nvm_bin

    logger.info("Starting vite server")
    vite = subprocess.Popen(
        [str(npx_path), "vite", "--clearScreen", "false"],
        cwd=pathlib.Path(__file__).parent.parent.parent / "backathon-web",
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
            http="h11",
            port=8000,
            log_level="info",
            log_config=backathon.cmdline.main.LOGGING_CONFIG,
            reload=True,
            timeout_graceful_shutdown=1,
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
