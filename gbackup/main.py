import argparse
import os.path
import sys
import logging

import django
from django.core.management import call_command

logger = logging.getLogger("gbackup.main")

def setup():
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "gbackup.settings")
    django.setup()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", dest="config",
                        help="Specify the configuration database",
                        )

    commands = parser.add_subparsers(dest='command')
    init = commands.add_parser("init", help="Initialize a new local database "
                                            "cache")

    options = parser.parse_args()

    dbpath = os.environ.get("GBACKUP_CONFIG")
    if options.config:
        dbpath = options.config
    if not dbpath:
        dbpath = "./config.gbackup"
    dbpath = os.path.abspath(dbpath)
    os.environ['GBACKUP_CONFIG'] = dbpath
    setup()
    logger.info("Using config database {}".format(
        dbpath
    ))

    if options.command == "init":
        call_command("migrate")

if __name__ == "__main__":
    main()
