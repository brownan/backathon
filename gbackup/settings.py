import sys
import uuid
import os

DATABASES = {
    'default': {
        #'ENGINE': 'django.db.backends.sqlite3',
        'ENGINE': 'gbackup.sqlite3_backend',
    }
}

if "GBACKUP_CONFIG" in os.environ:
    # Will not be set when running tests:
    DATABASES['default']['NAME'] = os.environ['GBACKUP_CONFIG']

INSTALLED_APPS = [
    'gbackup',
]
LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "color": {
            "()": "colorlog.ColoredFormatter",
            "format": "%(log_color)s%(levelname)-8s%(reset)s "
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
        "scanlog": {
            "format": "%(levelname)-8s %(message)s",
        }
    },
    "handlers": {
        "stderr": {
            "class": "logging.StreamHandler",
            "formatter": "color" if sys.stderr.isatty() else "nocolor",
        },
        "scanlog": {
            "class": "logging.FileHandler",
            "formatter": "scanlog",
            "filename": "scan.log",
            "mode": "w",
            "encoding": "UTF-8",
            "delay": True,
        }
    },
    "loggers": {
        "gbackup": {
            "level": "INFO",
        },
        "gbackup.restore": {
            "level": "INFO",
            "handlers": [],
            "propagate": False,
        },
        "gbackup.scan": {
            "level": "INFO",
            "handlers": ["scanlog"],
            "propagate": False,
        },
        "django.db": {
            "level": "WARNING",
        }
    },
    "root": {
        "level": "WARNING",
        "handlers": ["stderr"],
    }

}

# Set a secret key for this session
SECRET_KEY = str(uuid.uuid4())