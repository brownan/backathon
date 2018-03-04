import argparse
import os.path
import logging
import sys

import django
from django.core.management import load_command_class, find_commands, \
    BaseCommand
from django.apps import apps

logger = logging.getLogger("gbackup.main")

def setup():
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "gbackup.settings")
    django.setup()

def main():
    """Main entry point for the command line interface

    The workflow of this function follows a similar pattern as Django's
    MangementUtility.execute() in that an inital "fake" parser is used to
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
        usage="%(prog)s subcommand --config CONFIG [options] [args]",
        add_help=False,
    )
    parser.add_argument("--config")
    options, args = parser.parse_known_args(argv)

    # Set the path to the database from either the command line option or an
    # env var. It must be set one way or the other.
    if options.config:
        os.environ['GBACKUP_CONFIG'] = options.config
    if not "GBACKUP_CONFIG" in os.environ:
        if subcommand == "help":
            # Just going to display some help... set an in-memory database so
            # we don't run into any errors if something tries to do database
            # access
            os.environ['GBACKUP_CONFIG'] = ":memory:"
        else:
            parser.error("You must use --config or set the environment variable "
                         "GBACKUP_CONFIG")
    dbpath = os.environ['GBACKUP_CONFIG']

    # Special exception, all commands except for 'init' require the database
    # to exist.
    if (subcommand not in ['init', 'help'] and not os.path.exists(dbpath)):
        sys.stderr.write("Could not find config database: {}\n".format(dbpath))
        sys.stderr.write("Check the path, or if this is a new config you must run 'init'\n")
        sys.exit(1)

    setup()

    # Now that we've configured Django, we can import the rest of the modules
    # and configure the real parser specific for the given subcommand
    gbackup_config = apps.app_configs['gbackup']
    commands = find_commands(
        os.path.join(gbackup_config.path, 'management')
    )
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

    command_class = load_command_class("gbackup", subcommand)
    assert isinstance(command_class, BaseCommand)

    # Reconfigure the parser and re-parse the arguments
    parser = argparse.ArgumentParser(
        prog="{} {}".format(os.path.basename(argv[0]), subcommand),
        description=command_class.help or None,
    )
    parser.add_argument("-v", "--verbose", action="count", default=0)
    parser.add_argument("-q", "--quiet", action="store_true")
    parser.add_argument("--config", help="Path to the config database")
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
    logging.getLogger("gbackup").setLevel(level)

    logger.info("Using config database {}".format(
        dbpath
    ))

    command_class.handle(**vars(options))

if __name__ == "__main__":
    main()
