"""
This custom database backend inherits from the sqlite3 backend but adds
a few customizations for this application.

"""
from logging import getLogger
from django.db.backends.sqlite3 import base, schema

logger = getLogger("django.db")

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
            logger.info("Beginning Transaction Immediate")
            self.cursor().execute("BEGIN IMMEDIATE")
        else:
            logger.info("Beginning Transaction Deferred")
            self.cursor().execute("BEGIN")

    def _commit(self):
        logger.info("Commiting Transaction")
        super()._commit()

    def _rollback(self):
        logger.info("Rolling back transaction")
        super()._rollback()

    def get_new_connection(self, conn_params):
        """Gets a new connection and sets our connection-wide settings

        This enables some features we use that are disabled by default,
        and tunes a few parameters for performance.

        Note: make sure to explicitly close all cursors opened here.

        """
        conn = super().get_new_connection(conn_params)

        # Set the page size. In SQLite version 3.12.0 this was changed to a
        # default of 4096, but some distros still use older versions of SQLite.
        conn.execute("PRAGMA page_size=4096").close()

        # Similarly, the cache size had a different default on older
        # versions. The documentation currently recommends -2000, which sets
        # it to 2,000KB. (negative numbers set KB, positive numbers set the
        # number of pages)
        conn.execute("PRAGMA cache_size=-2000").close()

        # The write-ahead-log's main advantage is that connections in a write
        # transaction don't block other connections from reading the database.
        # It also may add some performance improvements, but as of writing
        # this, tests show performance is about the same.
        conn.execute("PRAGMA journal_mode=WAL").close()

        # The wal_autocheckpoint value is how many pages of data the WAL may
        # contain before SQLite will attempt to run a checkpoint operation
        # (copy pages from the WAL back to the database itself).
        #
        # 1000 is the default but it's set explicitly here as it's relevant
        # and we may want to tune it later. Multiply this by the page size to
        # get approximately how big the WAL will grow to before we incur the
        # cost of a checkpoint. Currently set to approx. 4MB
        #
        # The WAL may grow larger for a variety of reasons (such as a lot of
        # write transactions while a reader is blocking checkpoints),
        # but after a checkpoint the WAL will be truncated to
        # journal_size_limit (below). And of course after the last connection
        # closes the WAL is deleted entirely.
        conn.execute("PRAGMA wal_autocheckpoint=1000").close()

        # Limit the size of the WAL file. The file may grow larger than this
        # during a transaction or heavy use, and normally SQLite doesn't
        # truncate but merely overwrites unused space. This forces SQLite to
        # truncate instead of overwrite if the file grows larger than this.
        conn.execute("PRAGMA journal_size_limit=10000000").close()

        # Setting the synchronous mode to NORMAL sacrifices durability for
        # performance. Writes to the WAL are not synced, so writes may be lost
        # on a system crash or power failure, which feels acceptable for this
        # application. Writes to the database are still synced, and the WAL
        # is still synced before checkpoints. This can really help
        # performance on workloads with lots of small transactions.
        # This doesn't help with writes within a transaction because those
        # writes aren't synced even under the default synchronous=FULL mode.
        conn.execute("PRAGMA synchronous=NORMAL").close()

        # The following options can help improve performance for some kinds of
        # workloads, but according to some quick tests, performance is about
        # the same for this application. My guess is that performance is
        # limited by other IO and the database is not the main bottleneck.
        # I'm leaving these settings in the code but commented out so it's
        # easy to uncomment them for performance tests in the future.

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
