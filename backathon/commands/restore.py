import getpass

from ..encryption import DecryptionError
from .. import models
from . import CommandBase, CommandError


class Command(CommandBase):
    help = "Restore one or more files or directories"

    def handle(self, options):
        repo = self.get_repo()

        print("All snapshots:")
        print("ID\tSnapshot Name")
        print("--\t-------------")
        for ss in models.Snapshot.objects.using(repo.db).order_by("date"):
            print("{}\t{}".format(ss.id, "{} of {}".format(ss.date, ss.printablepath)))

        while True:
            num = input("Choose a snapshot to restore from> ")
            try:
                ss = models.Snapshot.objects.using(repo.db).get(id=num)
                break
            except models.Snapshot.DoesNotExist:
                print("No such snapshot")

        print("Base path of this snapshot is {}".format(ss.printablepath))

        dest_dir = self.input_local_dir_path(
            "Enter path to restore to (will overwrite existing files)"
        )

        root = ss.root

        if repo.encrypter.password_required:
            while True:
                pwd = getpass.getpass("Enter your repository password: ")
                print("Decrypting key...")
                try:
                    key = repo.encrypter.get_decryption_key(pwd)
                except DecryptionError:
                    print("Invalid password or corrupted keyfile. Try again")
                else:
                    break
        else:
            key = None

        print("Restoring files...")

        repo.restore(root, dest_dir, key=key)
