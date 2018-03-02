import argparse
import os.path
import logging
import sys

import django
from django.core.management import load_command_class, find_commands
from django.apps import apps

logger = logging.getLogger("gbackup.main")

def setup():
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "gbackup.settings")
    django.setup()

def main():
    """Main entry point for the command line interface"""
    # The workflow of this function follows the same pattern as Django's
    # ManagementUtility.execute()

    argv = sys.argv
    try:
        subcommand = argv[1]
    except IndexError:
        subcommand = "help"

    parser = argparse.ArgumentParser(
        usage="%(prog)s subcommand [options] [args]",
        add_help=False,
    )
    parser.add_argument("--config")

    options, args = parser.parse_known_args(argv[2:])
    if options.config:
        os.environ['GBACKUP_CONFIG'] = options.config

    if not "GBACKUP_CONFIG" in os.environ:
        parser.error("You must use --config or set the environment variable "
                     "GBACKUP_CONFIG")

    dbpath = os.environ['GBACKUP_CONFIG']
    logger.info("Using config database {}".format(
        dbpath
    ))

    # Special exception, all commands except for 'init' require the database
    # to exist.
    if (subcommand not in ['init']
            and not os.path.exists(dbpath)):
        sys.stderr.write("Could not find config database: {}".format(dbpath))
        sys.exit(1)

    setup()

    # Now that we've configured Django, we can import the rest of the modules
    # and configure the real parser specific for the given subcommand
    gbackup_config = apps.app_configs['gbackup']
    commands = find_commands(
        os.path.join(gbackup_config.path, 'management')
    )
    if subcommand not in commands:
        sys.stderr.write("Unknown command: {!r}\tType '{} help' for usage.\n"
                         .format(subcommand, os.path.basename(argv[0])))
        sys.exit(1)

    command_class = load_command_class("gbackup", subcommand)

    # Create the real parser
    parser = argparse.ArgumentParser(
        prog="{} {}".format(os.path.basename(argv[0]), subcommand)
    )
    parser.add_argument(
        "--config",
        help="Specify the configuration database to use. You can also set "
             "this with the GBACKUP_CONFIG environment variable. It must be "
             "set in one of those two ways."
    )
    command_class.add_arguments(parser)


if __name__ == "__main__":
    main()
