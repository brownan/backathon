import argparse
import os.path
import logging
import pkgutil
import sys
from importlib import import_module

import django
from django.apps import apps

from backathon.commands import CommandError

logger = logging.getLogger("backathon.main")

def setup():
    os.environ['DJANGO_SETTINGS_MODULE'] = os.environ.get(
        "BACKATHON_SETTINGS_MODULE", "backathon.settings"
    )
    django.setup()

def main():
    """Main entry point for the command line interface

    """

    setup()

    commands = {
        cmd_name: get_command_obj(cmd_name)
        for cmd_name in find_commands()
    }

    # Configure the parser
    parser = argparse.ArgumentParser(prog="Backathon")
    parser.add_argument("-v", "--verbose", action="count", default=0,
                        help="Increase logging verbosity. Use twice for debug"
                             "output")
    parser.add_argument("-q", "--quiet", action="store_true",
                        help="Disable all logging output except errors")

    # Add a subparser for each defined command class in the backathon.commands
    # package, and add each command's arguments to its subparser.
    subparsers = parser.add_subparsers(title="subcommands", dest="cmd_name")
    for cmd_name, command in commands.items():
        sub_parser = subparsers.add_parser(cmd_name,
                                           help=command.help)
        command.add_arguments(sub_parser)

    args = parser.parse_args()
    if args.cmd_name is None:
        parser.print_help()
        sys.exit(1)

    # Set log level
    if args.quiet:
        level = logging.ERROR
    elif args.verbose == 0:
        level = logging.WARNING
    elif args.verbose == 1:
        level = logging.INFO
    else:
        level = logging.DEBUG
    logging.getLogger("backathon").setLevel(level)

    command = commands[args.cmd_name]

    try:
        command.handle()
    except CommandError as e:
        logger.error(str(e))
        sys.exit(1)
    except KeyboardInterrupt:
        sys.exit(1)

def find_commands():
    backathon_config = apps.app_configs['backathon']
    path = os.path.join(backathon_config.path, "commands")
    return [name for _, name, is_pkg in pkgutil.iter_modules([path])
            if not is_pkg and not name.startswith("_")]

def get_command_obj(cmd_name):
    module = import_module('backathon.commands.{}'.format(cmd_name))
    return module.Command()


if __name__ == "__main__":
    main()
