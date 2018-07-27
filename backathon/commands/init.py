import getpass

from . import CommandBase
from .. import repository
from .. import encryption

class Command(CommandBase):
    help = "Initialize a new repository and local cache database"

    def handle(self, options):

        path = self.config

        print("Initializing a new database at {}...".format(path))

        repo = repository.Repository(path)

        if "STORAGE_SETTINGS" not in repo.settings:
            choice = self.input_menu(
                "Choose a destination for your backup storage",
                ["Local Filesystem", "Backblaze B2"],

            )
            if choice == 0:
                settings = {
                    'base_dir': self.input_local_dir_path("Enter a local path for the storage destination")
                }
                repo.set_storage('local', settings)
            elif choice == 1:
                settings = {
                    'account_id': input("Enter your account ID: "),
                    'application_key': input("Enter your application key: "),
                    'bucket_name': input("Enter your bucket name: "),
                }
                repo.set_storage('b2', settings)
            else:
                raise AssertionError()
        else:
            print("Storage settings are alreay configured. Skipping...")

        if "COMPRESSION_ENABLED" not in repo.settings:
            repo.set_compression(
                self.input_yn("Would you like to enable compression?", default=True)
            )
        else:
            print("Compression already configured. Skipping...")

        if "ENCRYPTION_SETTINGS" not in repo.settings:
            if self.input_yn("Would you like to enable encryption?", default=True):
                while True:
                    password = getpass.getpass()
                    if password != getpass.getpass("Repeat: "):
                        print("Passwords do not match")
                    else:
                        break
                print("Do not lose your password. You will need it to recover "
                      "files")
                print("Generating encryption keys...")
                encrypter = encryption.NaclSealedBox.init_new(password)
                repo.set_encrypter(encrypter)
            else:
                print("Encryption disabled. Remote repository contents will be")
                print("stored in plain text. Make sure you trust the destination!")
                repo.set_encrypter(encryption.NullEncryption.init_new())
        else:
            print("Encryption already configured. Skipping...")

        print()
        print("Saving metadata to storage repo...")
        repo.save_metadata()
        print()
        print("Done!")
        print("Next step: add some backup roots with the 'addroot' command")

