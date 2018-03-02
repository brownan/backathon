from django.core.management import BaseCommand

from gbackup.commands import Init

class Command(Init, BaseCommand):
    pass