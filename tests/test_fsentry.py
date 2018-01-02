from django.test import TestCase

from gbackup import models

class FSEntryTest(TestCase):

    def test_invalidate_parents(self):
        o = models.Object.objects.create(objid="a")

        root = models.FSEntry.objects.create(
            path="/1",
            objid=o,
        )
        e1 = models.FSEntry.objects.create(
            path="/1/2",
            parent=root,
            objid=o,
        )
        e2 = models.FSEntry.objects.create(
            path="/1/2/3",
            parent=e1,
            objid=o,
        )
        e3 = models.FSEntry.objects.create(
            path="/1/2/3/4",
            parent=e2,
            objid=None,
        )

        self.assertListEqual(
            list(models.FSEntry.objects.filter(objid__isnull=True)),
            [e3],
        )
        self.assertSetEqual(
            set(models.FSEntry.objects.filter(objid__isnull=False)),
            {root,e1,e2},
        )

        models.FSEntry.invalidate_parents()

        self.assertEqual(
            models.FSEntry.objects.filter(objid__isnull=True).count(),
            4
        )
