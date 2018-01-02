import os
import sys

import django

def setup():
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "gbackup.settings")
    django.setup()

