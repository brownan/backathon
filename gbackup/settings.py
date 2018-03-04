import sys
import uuid
import os

DATABASES = {
    'default': {
        #'ENGINE': 'django.db.backends.sqlite3',
        'ENGINE': 'gbackup.sqlite3_backend',

        # Default value is here for testing. All the normal entry points make
        # sure this environment variable is set before calling django.setup()
        'NAME': os.environ.get('GBACKUP_CONFIG',"db.sqlite3"),
    }
}

DEBUG = True

USE_TZ = True

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
    },
    "loggers": {
        "gbackup": {
            "level": "WARNING",
        },
        "gbackup.restore": {
            "level": "INFO",
            "handlers": [],
            "propagate": False,
        },
        "gbackup.scan": {
            "level": "INFO",
            "handlers": [],
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