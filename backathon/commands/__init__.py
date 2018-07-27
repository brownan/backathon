import pathlib

class CommandError(Exception):
    pass


class CommandBase:

    help = ""

    def add_arguments(self, parser):
        return parser

    def handle(self, options):
        raise NotImplementedError()

    def get_repo(self, options):
        from .. import repository
        return repository.Repository(options.config)

    def input_yn(self, prompt, default=None):
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

    def input_menu(self, prompt, choices):
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

    def input_local_dir_path(self, prompt):
        while True:
            response = input(prompt + ": ")
            path = pathlib.Path(response)
            if path.is_dir():
                return str(path)
            elif not path.exists() and path.parent.is_dir():
                if self.input_yn("Path does not exist. Create it?",
                                 default=True):
                    path.mkdir()
                    return str(path)
            else:
                print("Path does not exist")
