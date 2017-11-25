#!/usr/bin/env python3
from setuptools import setup

setup(
    name='gbackup',
    version='1.0',
    packages=['gbackup'],
    url='',
    license='',
    author='Andrew Brown',
    author_email='',
    description='',
    entry_points={
        'console_scripts': [
            'gbackup_makemigrations = gbackup.main:makemigrations',
        ]
    },
)
