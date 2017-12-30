import time

from gbackup import main
main.setup()

from gbackup import models

#models.FSEntry.objects.all().delete()

root = models.FSEntry.objects.get_or_create(path="/home/andrew/")

from gbackup import scan
t1 = time.time()
scan.scan()
t2 = time.time()
print("Scanned {} entries in {:.2f}s seconds".format(
    models.FSEntry.objects.count(),
    t2-t1,
))
