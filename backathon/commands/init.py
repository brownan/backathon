import argparse
import getpass
import os.path

from . import CommandBase, CommandError
from .. import repository
from .. import encryption

class Command(CommandBase):
    help = "Initialize a new repository and local cache database"

    @classmethod
    def add_arguments(cls, parser: argparse.ArgumentParser):
        parser.add_argument(
            "repo",
            help="Path to the metadata file to create and initialize",
            metavar="<repo>"
        )

        g = parser.add_mutually_exclusive_group(required=True)
        g.add_argument("--compression", action="store_true")
        g.add_argument("--no-compression", dest="compression", action="store_false")

        g = parser.add_argument_group(title="Storage Options")
        g.add_argument("--storage", required=True,
                       choices=['local', 'b2'],
                       )
        g.add_argument("--storage-path")
        g.add_argument("--b2-account-id")
        g.add_argument("--b2-application-key")
        g.add_argument("--b2-bucket-name")

        g = parser.add_argument_group(title="Encryption Options")
        e = g.add_mutually_exclusive_group()
        e.add_argument("--encryption", action="store_true")
        e.add_argument("--no-encryption", action="store_false", dest="encryption")

    def handle(self, args):

        path = args.repo
        if os.path.exists(path):
            raise CommandError("File already exists. Refusing to re-initialize")

        # Check some argument options for consistency, since argparse can't
        # track conditional dependencies
        if args.storage == "b2":
            if not args.b2_account_id:
                raise CommandError("Must specify --b2-account-id if using b2 storage")
            if not args.b2_application_key:
                raise CommandError("Must specify --b2-application-key if using b2 storage")
            if not args.b2_bucket_name:
                raise CommandError("Must specify --b2-bucket-name if using b2 storage")
            storage_settings = dict(
                account_id=args.b2_account_id,
                application_key=args.b2_application_key,
                bucket_name=args.b2_bucket_name
            )
        else:
            if not args.storage_path:
                raise CommandError("Must specify --storage-path if using local storage")
            storage_settings = dict(
                base_dir=args.storage_path
            )

        self.print("Initializing local metadata at {}...".format(path))

        if args.encryption:
            self.print("Do not lose your password. You will need it to recover "
                  "files")
            password = getpass.getpass("Enter a master password to generate an encryption key: ")
            password2 = getpass.getpass("Repeat: ")
            if password != password2:
                raise CommandError("Passwords do not match. Bailing")
            self.print("Generating encryption keys...")
            encrypter = encryption.NaclSealedBox.init_new(password)
        else:
            self.print("Encryption disabled. Remote repository contents will be")
            self.print("stored in plain text. Make sure you trust the destination!")
            encrypter = encryption.NullEncryption.init_new()

        repo = repository.Repository(path)
        repo.set_storage(args.storage, storage_settings)
        repo.set_compression(args.compression)
        repo.set_encrypter(encrypter)

        self.print("Initializing remote storage...")
        repo.save_metadata()
        self.print("Repository initialized!")

