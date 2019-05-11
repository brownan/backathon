import abc
import pathlib
from argparse import ArgumentTypeError
import os.path
import logging


class CommandError(Exception):
    pass


class CommandBase(metaclass=abc.ABCMeta):

    help = ""
    logger = logging.getLogger("backathon.cmd")

    def __init__(self):
        pass

    def add_arguments(self, parser):
        pass

    @abc.abstractmethod
    def handle(self, args):
        pass

    def print(self, s):
        self.logger.info(s)

def _backathon_type(dbname):
    if not os.path.exists(dbname):
        raise ArgumentTypeError("Database file {} does not exist".format(dbname))
    from backathon.repository import Backathon
    return Backathon(dbname)


class RepoCommand(CommandBase):

    def add_arguments(self, parser):
        super().add_arguments(parser)
        parser.add_argument("repo", type=_backathon_type,
                            help="Repository metadata file")


def input_yn(prompt, default=None):
    if default is None:
        prompt += " [y/n]: "
    elif default:
        prompt += " [Y/n]: "
    else:
        prompt += " [y/N]: "

    while True:
        response = input(prompt)

        if not response and default is not None:
            return default

        response = response.lower()
        if response not in "yn":
            print("Please type 'y' or 'n'")
        else:
            return response == 'y'

def input_menu(prompt, choices):
    while True:
        for i, choice in enumerate(choices):
            print("{}) {}".format(i+1, choice))

        response = input(prompt + ": ")

        try:
            response = int(response)
        except ValueError:
            print("Invalid choice")
            continue

        if response not in range(1, len(choices)+1):
            print("Invalid choice")
            continue

        return response-1

def input_local_dir_path(prompt):
    while True:
        response = input(prompt + ": ")
        path = pathlib.Path(response)
        if path.is_dir():
            return str(path)
        elif not path.exists() and path.parent.is_dir():
            if input_yn("Path does not exist. Create it?",
                        default=True):
                path.mkdir()
                return str(path)
        else:
            print("Path does not exist")

