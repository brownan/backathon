# Gbackup

Gbackup is a personal backup solution that has the following goals:

* Client side encryption
* Content-addressable storage system
* Runs as a daemon (no cobbling together of wrapper scripts)
* Built in backup scheduler (no more cron)
* Continuous file monitoring with inotify (no expensive scanning of the entire 
backup set)

No other backup programs quite met these criteria. Gbackup takes ideas from 
Borg, Duplicati, and others, with a backing storage format inspired by Git 
(hence the G in Gbackup)

## Architecture

These components make up Gbackup

### Object Store
An abstraction on top of a content-addressable block store. It provides 
encyption and compression, as well as several storage backends.

### Chunker
Takes a file on the filesystem and breaks it into chunks suitable for 
uploading to the object store.

### Filesystem

A set of filesystem objects represent objects on the filesystem and can 
convert into objects for storing in the object store. Filesystem objects may 
be cached on the local filesystem, and are backed by one or more objects in the 
object store.

* tree - a directory of files
* file - metadata about the file, and a list of blobs
* blob - file contents

### Revision

A revision is a single snapshot of a backup set. Metadata about the revision 
is stored in an object, and a link to the root of the backup tree. These are 
stored in a separate directory in the object store, and aren't content 
addressable. 

