from gbackup import main

main.setup()

from gbackup import models, backup
models.Object.objects.all().delete()

import shutil
shutil.rmtree("/tmp/gbackup_storage/objects")

backup.backup()