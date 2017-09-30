#!/usr/bin/env python

import unittest
import os.path
import sys

test_suite = unittest.defaultTestLoader.discover(
    os.path.join(os.path.dirname(__file__), 'tests')
)

runner = unittest.TextTestRunner()
result = runner.run(test_suite)
sys.exit(not result.wasSuccessful())
