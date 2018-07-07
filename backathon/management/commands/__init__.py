import os.path
import sys

from django.core.exceptions import ImproperlyConfigured
from django.core.management import BaseCommand, CommandParser, CommandError
from django.core.management.base import SystemCheckError
from django.db import connections

from ...repository import Repository

class BackathonCommand(BaseCommand):

    def create_parser(self, prog_name, subcommand):

        """
        Create and return the ``ArgumentParser`` which will be used to
        parse the arguments to this command.
        """
        parser = CommandParser(
            self, prog="%s %s" % (os.path.basename(prog_name), subcommand),
            description=self.help or None,
        )
        parser.add_argument("config")
        parser.add_argument("--scanlog", action="store")

        self.add_arguments(parser)
        return parser


    def execute(self, *args, **options):
        options['repo'] = Repository(options['config'])

        if options['scanlog']:
            import logging
            logger = logging.getLogger("backathon.scan")
            logger.addHandler(logging.FileHandler(options['scanlog']))

        self.handle(*args, **options)

    def run_from_argv(self, argv):
        self._called_from_command_line = True
        parser = self.create_parser(argv[0], argv[1])

        options = parser.parse_args(argv[2:])
        cmd_options = vars(options)
        # Move positional args out of options to mimic legacy optparse
        args = cmd_options.pop('args', ())
        try:
            self.execute(*args, **cmd_options)
        except Exception as e:
            if not isinstance(e, CommandError):
                raise

            # SystemCheckError takes care of its own formatting.
            if isinstance(e, SystemCheckError):
                self.stderr.write(str(e), lambda x: x)
            else:
                self.stderr.write('%s: %s' % (e.__class__.__name__, e))
            sys.exit(1)
        finally:
            try:
                connections.close_all()
            except ImproperlyConfigured:
                # Ignore if connections aren't setup at this point (e.g. no
                # configured settings).
                pass
