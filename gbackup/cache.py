import sqlite3

def get_db_conn(cachefile):
    conn = sqlite3.connect(cachefile)

    # Initialize tables
    conn.execute("""
CREATE TABLE IF NOT EXISTS config (key UNIQUE ON CONFLICT REPLACE, value);
""")

    return conn

class ObjectCache:
    CURRENT_SCHEMA_VERSION = 1

    def __init__(self, conn):
        self.conn = conn
        version = conn.execute("""
        SELECT value FROM conf WHERE key='objcachever';
        """).fetchone()
        if version is None or version[0] != self.CURRENT_SCHEMA_VERSION:
            conn.execute("INSERT INTO config VALUES ('objcachever', ?)",
                         (self.CURRENT_SCHEMA_VERSION,))

            conn.execute("""
            CREATE TABLE objects (
              objid PRIMARY KEY,
              payload
              );
            """)

            conn.execute("""
            CREATE TABLE objdeps (
              parent,
              child,
              UNIQUE (parent, child) ON CONFLICT IGNORE
              );
            """)

            conn.execute("""
            CREATE TABLE filecache(
              path UNIQUE ON CONFLICT REPLACE,
              inode,
              mtime,
              size,
              objid,
              ttl
            )
            """)
            self.conn.commit()

class FileCache:
    CURRENT_SCHEMA_VERSION = 1

    def __init__(self, conn):
        self.conn = conn
        version = conn.execute("""
        SELECT value FROM config WHERE key='filecachever';
        """).fetchone()
        if version is None or version[0] != self.CURRENT_SCHEMA_VERSION:
            conn.execute("INSERT INTO config VALUES ('filecachever', ?)",
                         (self.CURRENT_SCHEMA_VERSION,))
            conn.execute("""DROP TABLE IF EXISTS filecache""")
            conn.execute("""
            CREATE TABLE filecache(
              path UNIQUE ON CONFLICT REPLACE,
              inode,
              mtime,
              size,
              objid,
              ttl
            )
            """)

    def get_file_cache(self, path, inode, mtime, size):
        """Checks the file cache for a matching file

        If the file exists, returns its hash. Otherwise, returns None
        """
        row = self.conn.execute("""
            SELECT objid FROM filecache
            WHERE path=? AND inode=? AND mtime=? AND size=?
            """, (path, inode, mtime, size)).fetchone()
        if row is None:
            return None
        else:
            return row[0]

    def set_file_cache(self, path, inode, mtime, size, objid):
        self.conn.execute("""
          INSERT INTO filecache
          VALUES (?, ?, ?, ?, ?, ?)
        """, (path, inode, mtime, size, objid, 25))
        self.conn.commit()

    def decrement_ttl(self):
        """Decrements the TTLs for each entry in the file cache. If a ttl
        reaches 0, it is removed from the file cache

        """
        self.conn.execute("""
            UPDATE filecache SET ttl=ttl-1
        """)
        self.conn.execute("""
            DELETE FROM filecache WHERE ttl<0
        """)
        self.conn.commit()

