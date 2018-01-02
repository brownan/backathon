#!/usr/bin/env python3

import sys

from django.core.management import execute_from_command_line

from gbackup import main
main.setup()

execute_from_command_line(sys.argv)
