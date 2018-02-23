#!/usr/bin/env python3

import sys
import os

from django.core.management import execute_from_command_line

os.environ.setdefault("GBACKUP_CONFIG", "./config.gbackup")

from gbackup import main
main.setup()

execute_from_command_line(sys.argv)
