import copy
import logging
import os
import pathlib
import subprocess
import sys
import time

import typer
import uvicorn.config

from backathon.cmdline.types import PathOption

logger = logging.getLogger("backathon.server")

app = typer.Typer()

LOGGING_CONFIG = copy.deepcopy(uvicorn.config.LOGGING_CONFIG)
LOGGING_CONFIG["handlers"]["default"] = {"class": "rich.logging.RichHandler"}
LOGGING_CONFIG["loggers"]["backathon"] = {"level": "INFO"}
LOGGING_CONFIG["root"] = {"handlers": ["default"]}


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
            port=8000,
            log_level="info",
            log_config=LOGGING_CONFIG,
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
