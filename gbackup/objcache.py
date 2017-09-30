import sqlite3

class ObjCache:
    """The object cache stores Object hashes and contents locally

    The cache's main purpose is to avoid making network calls for object data
    in a remote store. It also builds indices for quickly determining:

    * Whether an object exists in the store
    * Traversing the entire tree to determine which objects are not
      accessible from the given root(s)
    * Matching files to existing objects so they don't have to be re-hashed
      in their entirety

    """
    def __init__(self, cachefile):
        conn = self.conn = sqlite3.connect(cachefile)

        # Initialize tables
        conn.execute("""
CREATE TABLE IF NOT EXISTS conf (key, value);
""")
        version = conn.execute("""
        SELECT value FROM conf WHERE key='version';
        """).fetchone()
        if version is None:
            conn.execute("INSERT INTO conf VALUES ('version', 1);")

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
            CREATE TABLE filemetadata (
              path UNIQUE ON CONFLICT REPLACE,
              inode,
              mtime,
              size,
              objid
            )
            """)
            self.conn.commit()

    def get_file_cache(self, path, inode, mtime, size):
        """Checks the file cache for a matching file

        If the file exists, returns its hash. Otherwise, returns None
        """
        return self.conn.execute("""
            SELECT objid FROM filemetadata
            WHERE path=? AND inode=? AND mtime=? AND size=?
            """, (path, inode, mtime, size)).fetchone()

    def set_file_cache(self, path, inode, mtime, size, objid):
        self.conn.execute("""
          INSERT INTO filemetadata
          VALUES (?, ?, ?, ?, ?)
        """, (path, inode, mtime, size, objid))
        self.conn.commit()
