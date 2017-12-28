# Gbackup

Gbackup is a personal backup solution that has the following goals:

* Client side encryption
* Content-addressable storage system
* Runs as a daemon (no cobbling together of wrapper scripts)
* Built in backup scheduler (no more cron)
* Continuous file monitoring with inotify (no expensive scanning of the entire 
backup set)
* Backup and prune operations do not require the secret encryption key
* Verify and restore operations require the encryption secret key

No other backup programs I've found quite met these criteria. Gbackup takes 
ideas from Borg, Duplicati, Bup, and others, with a backing storage format 
inspired by Git (hence the G in Gbackup)

## Architecture

The backup procedure consists of 3 processes, described here.

### 1. Scan process

We effectively perform a breadth-first-search on the tree, but iteratively, 
not recursively.

First iteration: The root is the only object in the table, and it is selected 
and updated. Root's entry is updated with its os.lstat() information, and its 
children are enumerated and inserted into the FSEntry table with no other 
metadata. 

Second iteration: The children are selected (all objects that have no 
metadata are selected. TODO: what is the selection criteria?). Those objects 
are updated, which runs os.lstat() and gets their information and updates 
those database entries. *If the object's metadata changes, its objid is set 
to NULL indicating the cache is invalid and it needs updating next backup.* Any 
new directory entries have listdir() called and their children are inserted 
into the database with no metadata.

Iterations continue until there are no new entries with no metadata selected 
from the database. At this point, the database's cached representation of the
filesystem is complete.

Note: The first-ever scan initially selects only the root node, but 
subsequent scans will select *every* node for the first iteration. Further 
iterations will catch new files and directories.

### 2. Invalidation process

Any objects in the FSEntry table that don't have an object ID indicate they 
have no backing object in the remote store. This could be because they are 
new objects or because they have changed (as determined by the scan process).

Problem is, changed objects don't automatically invalidate their parent 
objects. This process scans the table and invalidates parent entries whose 
children have no object ID.

### 3. Backup process

Once the scan is complete, the tree is recursively traversed in a 
depth-first-search post-order traversal (so leaves are enumerated first). This
lets us upload objects that need uploading, and return object IDs to the 
parents for their use. It also lets us avoid traversing down branches that 
already have an object ID, and thus don't need updating (as determined by the
scan process)

### About the processes

Because there may be quite some time between a scan and the backup, FSEntry 
nodes are updated again with the latest metadata when an entry is backed up. 
The scan process may thus seem redundant, but it is still necessary to 
discover nodes deep in the tree that need updating, which we wouldn't find 
with a simple tree traversal unless we traversed the *entire* tree.

While the scan process effectively *is* traversing the entire tree (just not 
in any particular order), this is easier to do efficiently than a recursive 
tree traversal. Traversing the *entire* tree recursively is a heavy database 
load since each recursive call has to do a database query to discover the 
children. With this architecture, it's much quicker because the first 
iteration is a single query and updates the vast majority of entries.

I experimented with using a raw, recursive SQLite query to get all the 
nodes in a post-order traversal, but that has some gotchas with integrating 
with Django, as Django doesn't stream results from RawQuerySet querysets from
SQLite [1]. There are also gotchas with updating a table while reading from the 
same table in SQLite [2], which is probably why Django doesn't stream entries
from sqlite cursors.

[1] https://github.com/django/django/blob/master/django/db/backends/sqlite3/features.py#L7

[2] https://www.sqlite.org/isolation.html

Also note that while the Django documentation says SQLite doesn't 
support streaming results at all [3], it does actually efficiently execute 
querysets when you use the .iterator() method. I'm not clear if this is a 
mistake in the Django implementation or docs, but according to my tests, we can 
efficiently execute large queries with regular (non-Raw) querysets with 
Django on SQLite. We still need to be aware of the isolation limitations 
noted in [2] though.

[3] https://docs.djangoproject.com/en/2.0/ref/models/querysets/#without-server-side-cursors