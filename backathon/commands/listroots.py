from backathon.commands import CommandBase


class Command(CommandBase):
    help = "Lists currently defined backup roots"

    def handle(self, *args, **kwargs):
        for entry in self.get_repo().get_roots():
            print(entry.printablepath)
