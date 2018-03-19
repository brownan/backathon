"""
This custom database backend inherits from the sqlite3 backend but adds
a few customizations for this application.

In particular, it adds:
* Cascading deletes to foreign key relations. Foreign key fields with on_delete
  set to DO_NOTHING will efficiently delete all objects referencing the deleted
  object without pulling them into memory. Signals and such aren't run, but for
  large tables, this is the only way to delete a large number of objects
  without blowing up memory usage and taking forever.
* Uses the Write-ahead Log journal mode to support readers reading
  simultaneously with a single writer.
* Supports starting transactions with BEGIN IMMEDIATE for immediately
  acquiring the RESERVED lock

"""
from django.db.backends.sqlite3 import base, schema

class DatabaseSchemaEditor(schema.DatabaseSchemaEditor):
    sql_create_inline_fk = "REFERENCES %(to_table)s (%(to_column)s) " \
                           "ON DELETE CASCADE " \
                           "DEFERRABLE INITIALLY DEFERRED"

class DatabaseWrapper(base.DatabaseWrapper):
    SchemaEditorClass = DatabaseSchemaEditor

    def __init__(self, *args, **kwargs):
        # Set by a custom context manager to tell us to use BEGIN IMMEDIATE
        # when beginning a transaction
        self.begin_immediate = False
        super().__init__(*args, **kwargs)

    def get_new_connection(self, conn_params):
        """Enable a couple sqlite features that are disabled by default"""
        conn = super().get_new_connection(conn_params)

        # The write-ahead-log doesn't add much performance to our use case.
        # The main advantage is that it allows readers while another
        # connection is in a write transaction. Since there's typically just
        # one process accessing the database, this is mainly useful for
        # debugging: we can access the database while a scan or some other
        # big operation is running
        conn.execute("PRAGMA journal_mode=WAL").close()

        # The .close() fixes the cursor being left open causing SQLITE_BUSY
        # on later commits. This is only a problem on PyPy.

        # Memory-mapped IO can help performance on read-heavy loads by
        # avoiding a lot of read() system calls, but according to some quick
        # tests it doesn't speed up the code much, despite almost cutting the
        # number of system calls in half during a scan.
        # The only problem with leaving this on is that Linux counts shared
        # memory toward's a process's RSS usage, making this process look
        # like it's using more memory than it actually is. So I keep this off
        # for development so it's easy to see if a routine or query uses
        # more memory than I expect.
        #conn.execute("PRAGMA mmap_size=1073741824;").close()
        return conn

    def _start_transaction_under_autocommit(self):
        if self.begin_immediate:
            self.cursor().execute("BEGIN IMMEDIATE")
        else:
            self.cursor().execute("BEGIN")