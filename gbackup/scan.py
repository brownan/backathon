from tqdm import tqdm

from . import models

def scan():
    """Scans all FSEntry objects for changes


    """

    qs = models.FSEntry.objects.all()
    pass_ = 0
    while qs.exists():

        pass_ += 1
        count = qs.count()

        for entry in tqdm(
            qs.iterator(),
            desc="Pass {}".format(pass_),
            total=count,
            unit='entries',
        ):
            entry.scan()

        qs = models.FSEntry.objects.filter(new=True)
