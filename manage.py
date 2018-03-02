#!/usr/bin/env python3

import sys
import os

from django.core.management import execute_from_command_line

from gbackup import main

if __name__ == "__main__":
    main.setup()

    execute_from_command_line(sys.argv)
