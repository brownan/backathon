import sys

import django
from django.conf import settings
from django.core.management import execute_from_command_line, call_command

def setup():
    settings.configure(
        DATABASES={
            'default': {
                'ENGINE': 'django.db.backends.sqlite3',
                'NAME': "database.sqlite3",
            }
        },
        INSTALLED_APPS=[
            'gbackup',
        ],
        LOGGING={
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "color": {
                    "()": "colorlog.ColoredFormatter",
                    "format": "%(log_color)s%(levelname)-8s%(reset)s [%("
                              "name)s] "
                              "%(message)s",
                    "log_colors": {"DEBUG": "cyan", "INFO": "white",
                                   "WARNING": "yellow", "ERROR": "red",
                                   "CRITICAL": "white,bg_red",
                                   },
                },
                "nocolor": {
                    "format": "%(asctime)s %(levelname)-8s [%(name)s] %(message)s",
                    "datefmt": "%Y-%m-%d %H:%M:%S",
                },
            },
            "handlers": {
                "stderr": {
                    "class": "logging.StreamHandler",
                    "formatter": "color" if sys.stderr.isatty() else "nocolor",
                },
            },
            "loggers": {
                "gbackup": {
                    "level": "INFO",
                },
                "gbackup.scan": {
                    "level": "WARNING",
                }
            },
            "root": {
                "level": "WARNING",
                "handlers": ["stderr"],
            }

        },
    )
    django.setup()
    call_command("migrate", "-v0", "--noinput")

def manage():
    setup()
    execute_from_command_line(sys.argv)
