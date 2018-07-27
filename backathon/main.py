import argparse
import os.path
import logging
import pkgutil
import sys
from importlib import import_module

import django
from django.apps import apps

from .commands import CommandError, CommandBase

logger = logging.getLogger("backathon.main")

def setup():
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "backathon.settings")
    django.setup()

def main():
    """Main entry point for the command line interface

    The workflow of this function follows a similar pattern as Django's
    MangementUtility.execute() in that an initial "fake" parser is used to
    parse a couple preliminary arguments, but we always force the very first
    argument to be the subcommand, never an option.
    It also only exports commands from this package.
    """

    argv = sys.argv

    try:
        subcommand = argv[1]
    except IndexError:
        subcommand = "help"

    parser = argparse.ArgumentParser(
        usage="%(prog)s subcommand CONF_DB [options] [args]",
        add_help=False,
    )
    parser.add_argument("config")
    options, args = parser.parse_known_args(argv)

    dbpath = options.config

    # Special exception, all commands except for 'help' and 'init' require the
    # database to exist.
    if subcommand not in ['init', 'help'] and not os.path.exists(dbpath):
        sys.stderr.write("Could not find config database: {}\n".format(dbpath))
        sys.stderr.write("Check the path, or if this is a new config you must run 'init'\n")
        sys.exit(1)

    setup()

    commands = find_commands()

    if subcommand == "help":
        usage = [
            parser.usage % {'prog': parser.prog},
            "",
            "Available subcommands:"
        ]
        for command in sorted(commands):
            usage.append("\t" + command)
        sys.stdout.write("\n".join(usage) + "\n")
        sys.exit(1)

    if subcommand not in commands:
        sys.stderr.write("Unknown command: {!r}\tType '{} help' for usage.\n"
                         .format(subcommand, os.path.basename(argv[0])))
        sys.exit(1)

    command_class = get_command_class(subcommand)
    assert isinstance(command_class, CommandBase)

    # Reconfigure the parser and re-parse the arguments
    parser = argparse.ArgumentParser(
        prog="{} {}".format(os.path.basename(argv[0]), subcommand),
        description=command_class.help or None,
    )
    parser.add_argument("config", help="Path to the config database",
                        metavar="CONF_DB")
    parser.add_argument("-v", "--verbose", action="count", default=0)
    parser.add_argument("-q", "--quiet", action="store_true")
    command_class.add_arguments(parser)

    options = parser.parse_args(argv[2:])

    # Set log level
    if options.quiet:
        level = logging.ERROR
    elif options.verbose == 0:
        level = logging.WARNING
    elif options.verbose == 1:
        level = logging.INFO
    else:
        level = logging.DEBUG
    logging.getLogger("backathon").setLevel(level)

    logger.info("Using config database {}".format(
        dbpath
    ))

    try:
        command_class.handle(options)
    except CommandError as e:
        logger.error(str(e))
        sys.exit(1)

def find_commands():
    backathon_config = apps.app_configs['backathon']
    path = os.path.join(backathon_config.path, "commands")
    return [name for _, name, is_pkg in pkgutil.iter_modules([path])
            if not is_pkg and not name.startswith("_")]

def get_command_class(cmd_name):
    module = import_module('backathon.commands.{}'.format(cmd_name))
    return module.Command()


if __name__ == "__main__":
    main()
