import sys
import uuid

DATABASES = {
    # Default database is used for process-wide configuration. Separate DB
    # connections are used for the cache tables, initialized by the
    # Repository class.
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': ":memory:",
    }
}

DEBUG = True

USE_TZ = True

INSTALLED_APPS = [
    'backathon',
]

DATABASE_ROUTERS = [
    'backathon.dbrouter.BackathonRouter',
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
        "backathon": {
            "level": "WARNING",
        },
        "backathon.restore": {
            "level": "INFO",
            "handlers": [],
            "propagate": False,
        },
        "backathon.scan": {
            "level": "INFO",
            "handlers": [],
            "propagate": False,
        },
        "django.db": {
            "level": "WARNING",
        },
        "django": {
            "handlers": [],
        }
    },
    "root": {
        "level": "WARNING",
        "handlers": ["stderr"],
    }

}

# Set a secret key for this session
SECRET_KEY = str(uuid.uuid4())