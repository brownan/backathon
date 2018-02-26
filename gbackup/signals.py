from django.core import signals

db_setting_changed = signals.Signal(
    providing_args=['setting', 'value']
)
