import pathlib
import getpass
import logging

from django.core.management import BaseCommand, CommandError

from ... import models
from ...datastore import default_datastore
from ... import restore

class Command(BaseCommand):
    help = "Restore one or more files or directories"

    def handle(self, *args, **options):
        self.stdout.write("All snapshots:")
        print("ID\tSnapshot Name")
        print("--\t-------------")
        for ss in models.Snapshot.objects.order_by("date"):
            print("{}\t{}".format(
                ss.id,
                "{} of {}".format(ss.date, ss.printablepath)
            ))

        while True:
            num = input("Choose a snapshot to restore from> ")
            try:
                ss = models.Snapshot.objects.get(id=num)
                break
            except models.Snapshot.DoesNotExist:
                print("No such snapshot")

        print("Base path of this snapshot is {}".format(ss.printablepath))
        print("Type a path relative to the base path to restore. Blank to "
              "restore everything")
        to_restore = input("Relative path> ")

        dest_dir = pathlib.Path(input(
            "Enter path to restore to (will overwrite existing files)> "
        ))
        if not dest_dir.parent.is_dir():
            raise CommandError(
                "{} does not exist".format(dest_dir.parent)
            )
        if not dest_dir.exists():
            dest_dir.mkdir()

        root = ss.root
        for component in pathlib.PurePath(to_restore).parts:
            try:
                root = root.get_child_by_name(component)
            except models.Object.DoesNotExist:
                self.stdout.write("Directory not found: {}".format(component))
            except ValueError:
                self.stdout.write("Not a directory: {}".format(component))

        if default_datastore.key_required:
            print("Enter your encryption password")
            pwd = getpass.getpass()
            self.stdout.write("Decrypting key...")
            key = default_datastore.get_local_privatekey(pwd)
        else:
            key = None

        self.stdout.write("Restoring files...")

        logging.getLogger("backathon.restore").addHandler(
            logging.StreamHandler()
        )
        restore.restore_item(root, dest_dir, key)
