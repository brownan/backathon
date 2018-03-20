"""
This custom database backend inherits from the sqlite3 backend but adds
a few customizations for this application.

"""
from django.db.backends.sqlite3 import base, schema

class DatabaseSchemaEditor(schema.DatabaseSchemaEditor):
    # This adds cascading deletes to foreign key relations. Foreign key
    # fields with on_delete set to DO_NOTHING will still cascade, but SQLite
    # will do the cascading, without Django having to pull all rows into
    # memory. Signals and such aren't run, but for large tables, this is the
    # only way to delete a large number of objects without using a lot of
    # memory.
    sql_create_inline_fk = "REFERENCES %(to_table)s (%(to_column)s) " \
                           "ON DELETE CASCADE " \
                           "DEFERRABLE INITIALLY DEFERRED"

class DatabaseWrapper(base.DatabaseWrapper):
    SchemaEditorClass = DatabaseSchemaEditor

    def __init__(self, *args, **kwargs):
        # This flag is set by a custom context manager to tell us to use BEGIN
        # IMMEDIATE when beginning a transaction
        self.begin_immediate = False
        super().__init__(*args, **kwargs)

    def _start_transaction_under_autocommit(self):
        if self.begin_immediate:
            self.cursor().execute("BEGIN IMMEDIATE")
        else:
            self.cursor().execute("BEGIN")

    def get_new_connection(self, conn_params):
        """Enable some sqlite features that are disabled by the default"""
        conn = super().get_new_connection(conn_params)

        # Set the page size. In SQLite version 3.12.0 this was changed to a
        # default of 4096, but some distros still use older versions of SQLite.
        conn.execute("PRAGMA page_size=4096").close()
        conn.execute("PRAGMA page_cache=-2000").close()

        # The write-ahead-log's main advantage is that it allows readers
        # while another connection is in a write transaction. It also may add
        # some performance improvements, but at the moment, tests show
        # performance is about the same.
        conn.execute("PRAGMA journal_mode=WAL").close()

        # The next two options can help improve performance for some kinds of
        # workloads, but according to some quick tests, performance is about
        # the same for this application. My guess is that performance is
        # limited by other IO and the database is not the main bottleneck.
        # I'm leaving these settings in the code but commented out so it's
        # easy to uncomment them for performance tests in the future.

        # Setting the synchronous mode to NORMAL sacrifices durability for
        # performance. Writes to the WAL are not synced, so writes may be lost
        # on a system crash or power failure, which feels acceptable for this
        # application. Writes to the database are still synced, and the WAL
        # is still synced before checkpoints.
        #conn.execute("PRAGMA synchronous=NORMAL").close()

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
