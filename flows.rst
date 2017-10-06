Caches
------
* Obj cache
  - Tracks the existence of objects in the remote store
  - holds the entire decrypted, decompressed object payload for every object
    except blob objects

* File cache
  - Tracks files in the backup set for changes
  - Maps (sha1(path), inode, mtime, size) to obj id
  - if an entry exists, it is assumed not to have changed on the filesystem
  - Keeps most recent version of seen files in this cache
  - a TTL keeps files that haven't been seen for a while. (would it be better
   to just delete cache entries if a file isn't seen? maybe unless the file
   cache is shared among multiple backup sets)

* Object Relations
  - Many-to-many relation of objects
  - Forms a directed acyclic dependency graph
  - Used to recursively traverse dependencies locally and calculate a set of
    unreachable objects for deletion.

Flow Summary
------------
The flows are high level operations that can be activated on the graph of
objects. The flows have different meanings for each object, but their high
level concepts are described here.

* Update - Used to re-scan the local filesystem for changes and incorporate
  them into the in-memory object.

  Goals:
  - This operation makes explicit the action of re-reading the filesystem.
  - Normal backup operations shouldn't touch the filesystem unless they have
    a reason to believe they need to.
  - Calling update() checks the filesystem (with a stat or listdir operation)
    to see if re-reading and hashing data is necessary.

* Backup - reads from local filesystem, creates objects, and pushes objects to
  remote object store.

  Goals:
  - Local data that has not changed should not be re-read from local storage
  - Local data that already exists in remote store should not transferred
  - Local data is assumed not to have changed unless a call to update() was
    made

* Restore - Reads data from remote object store and writes to local files

  Goals:
  - Restore to original paths or a new root directory
  - Missing or corrupt objects should not crash entire process. Should restore
    as much good data as possible, even partial files

* Verify - Verify the remote object store is consistent and well formed, and
  rebuild the object cache

  Goals:
    - uses a list of tree roots (indicating different backups), traverses
      remote objects to verify all referenced objects exist, and their hashes match.
    - Partial verify just verifies tree and inode objects, verifies the
      existence only of blob objects, and rebuilds local cache. Saves on
      bandwidth by only downloading metadata.
    - full verify also reads all blob objects and verifies their hash. Takes
      more bandwidth.
    - Result should be a rebuilt cache and a report of any bad backups and what
      objects are missing or failed validation.

* Backup delete - delete a backup from remote store

  Goals:
    - Use the object relation cache to calculate garbage objects
    - remove objects from the remote object store

  Note: this isn't a flow that is implemented recursively on the object
    classes. It is a graph traversal algorithm that runs on the cache to calculate
    a deletion set.


Flows for tree objects
----------------------

* Backup (Initialize with a directory path)
  - List contents in directory. For each item:
    - If is a directory: initialize new tree object and call BACKUP on it
    - if is a file: initialize inode object and call BACKUP on it
  - get object id from each directory item (returned from BACKUP call above)
  - sort list of (path, objid) items (for consistent ordering)
  - Construct tree object payload
  - hash contents to get obj id
  - if obj id is in cache, do nothing
  - if obj id is not in cache, upload and update obj cache
  - return obj id

* Restore (initialized with hash id, and local path)
  - fetch metadata from object cache
  - if tree obj does not exist in obj cache, fetch obj from remote store
  - create directory on local filesystem. Restore as many properties as possible
  - for each file, create an Inode object and call RESTORE

* Verify (initialize with an object id)
  - fetch metadata from remote object store
  - if remote object doesn't exist or can't be decrypted, or the contents hash
    don't match the object id, then log an error and return
  - update local object cache
  - for each entry, construct a tree or inode object and call VERIFY

Flows for Inode objects
-----------------------

* Update (initialized with a file path)
  - perform stat on file and determine if we have a file cache entry for the
    (path,inode,mtime,size) tuple.
  - If so, caches the resulting object ID as an instance variable

* Backup (initialized with a file path and optionally an object ID)
  - This is a generator function. Yields object contents to upload. Returns
    own object ID

  - Local object ID is set: assume no changes
    - implies all child object are uploaded, since they have been hashed and
      this object exists
    - nothing to do
    - return obj key for this inode object

  - Local object ID is NOT set
    - The file may or may not need uploading, but it must be scanned and hashed
    - Need to chunk the file, and then run BACKUP on each blob object
    - Build inode object payload
    - hash inode payload to get obj key
    - upload inode payload to remote store
    - add to obj cache
    - update file cache
    - return obj key for this inode object

* Restore (initialized with an object id, and a local path which may not exist)
  - Fetch file metadata from object cache.
  - If inode object does not exist in the object cache, fetch object from remote
    store
  - Create a new empty file on local filesystem with as many properties as can
    be restored
  - Open a file handle to the file, and for each blob object:
    - seek to the position in the file
    - call restore on the blob object

* Verify (initialize with an object id)
  - fetch metadata from remote object store
  - If remote object doesn't exist or can't be decrypted or the contents hash
    don't match the object id, then log an error and return
  - update local object cache
  - for each blob, call VERIFY on the blob

Flows for Blob objects
----------------------

* Backup (initialized with a blob of data)
  - hash blob to determine the key in the obj store
  - check obj cache

  - Blob obj in cache
    - assume it's been uploaded. Nothing to do.
    - return obj id
  
  - Blob obj not in cache
    - check if blob obj in remote storage
    - if not, upload blob
    - add entry to obj cache
    - return obj id

* Restore (initialize with an object id and a file object)
  - fetch blob payload from remote store
  - writes the blob contents to the file object

* Verify (given a object id)
  - if a quick verify is requested
    - verify a remote object with the given object id exists in the remote
      object store. Does not download. If not, log an error and return
    - Update object cache (only if no error)

  - If a full verify is requested:
    - Fetches remote payload for the given object id
    - if remote object doesn't exist, or can't be decrypted, or the contents
      hash don't match the object id, then log an error and return.
    - Update object cache (only if no error)
