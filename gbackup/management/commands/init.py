import argparse
import getpass

from django.core.management import BaseCommand

class Command(BaseCommand):
    help = "Initialize a new repository and local cache database"

    def add_arguments(self, parser: argparse.ArgumentParser):
        parser.add_argument("-c", "--compression",
                            help="Set compression mode",
                            choices=['none', 'zlib'],
                            action="store",
                            required=True,
                            )
        parser.add_argument("-e", "--encryption",
                            help="Set encryption mode",
                            choices=['none', 'nacl'],
                            action="store",
                            required=True,
                            )
        parser.add_argument("-s", "--storage",
                            required=True,
                            choices=['local'],
                            help="The storage backend to use",
                            )
        parser.add_argument("-p", "--path",
                            help="The storage path if using local storage",
                            required=True,
                            )

    def handle(self, **options):

        # First, migrate the database to create it and the tables
        from django.core.management import call_command
        call_command("migrate")

        if options['encryption'] == "nacl":
            print("Enter a password to secure your encryption keys")
            print("Keep this password safe. You will need it to restore files")
            password = getpass.getpass()
            if password != getpass.getpass("Repeat: "):
                print("Passwords do not match")
                return
        else:
            password = None

        # Now initialize things
        from gbackup.datastore import default_datastore
        self.stderr.write("Initializing database...")
        default_datastore.initialize(
            encryption=options['encryption'],
            compression=options['compression'],
            repo_backend=options['storage'],
            repo_path=options['path'],
            password=password,
        )
