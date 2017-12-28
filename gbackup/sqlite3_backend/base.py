"""
This custom database backend inherits from the sqlite3 backend but adds
cascading deletes to foreign key relations. Foreign key fields with on_delete
set to DO_NOTHING will efficiently delete all objects referencing the deleted
object without pulling them into memory. Signals and such aren't run, but for
large tables, this is the only way to delete a large number of objects
without blowing up memory usage and taking forever.

"""
from django.db.backends.sqlite3 import base, schema

class DatabaseSchemaEditor(schema.DatabaseSchemaEditor):
    sql_create_inline_fk = "REFERENCES %(to_table)s (%(to_column)s) " \
                           "ON DELETE CASCADE " \
                           "DEFERRABLE INITIALLY DEFERRED"

class DatabaseWrapper(base.DatabaseWrapper):
    SchemaEditorClass = DatabaseSchemaEditor
