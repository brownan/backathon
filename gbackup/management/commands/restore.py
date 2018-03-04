from django.core.management import BaseCommand

from gbackup import models

class Command(BaseCommand):
    help = "Restore one or more files or directories"

    def handle(self, *args, **options):
        self.stdout.write("All snapshots:")
        print("ID\tSnapshot Name")
        print("--\t-------------")
        for ss in models.Snapshot.objects.order_by("date"):
            print("{}\n{}".format(
                ss.id,
                "{} of {}".format(ss.date, ss.printablepath)
            ))
