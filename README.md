# Gbackup

Gbackup is a personal backup solution that has the following goals:

* Client side encryption
* Content-addressable storage system
* Runs as a daemon (no cobbling together of wrapper scripts)
* Built in backup scheduler (no more cron)
* Continuous file monitoring with inotify (no expensive scanning of the entire 
backup set)

No other backup programs quite met these criteria. Gbackup takes ideas from 
Borg, Duplicati, Bup, and others, with a backing storage format inspired by Git 
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

### Objects

There are three types of objects in the object store:
* Tree, corresponding to a directory entry. It links to other trees and inodes
* inode, corresponding to a file. Holds all the metadata of a file, and a links
  to a list of blob objects
* blob holds actual file data. Blobs are typically not complete files, but some
  large chunk of a file.
  
The object format is an optionally encrypted and compressed stream of msgpack
objects. The first object is a single byte 't', 'i', or 'b' describing the type
of object.
Each subsequent msgpack object is a tuple where the first item is a byte string
describing the property, and subsequent items in the tuple are data.

For example, a tree object looks like this (one line per msgpack object)
```
't'
('e', 'file1', 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855')
('e', 'file2', 'ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb')
```
where the 'e' property corresponds to a directory entry. This leaves the format
extensible for more metadata properties in the future.

Tree properties:
* 'u' user id
* 'g' group id
* 'm' directory mode
* 'e' a filesystem entry. Consists of a name and a hash to an inode or tree object.

Inode properties:
* 's' total file size in bytes
* 'i' inode number on the source filesystem. This may be useful for reconstructing
  hard links when restoring files.
* 'u' user id
* 'g' group id
* 'm' file mode
* 'ct' ctime
* 'mt' mtime
* 'd' data chunks. Attributes are: offset, object hash to a blob. May contain many
  data chunks to reconstruct the file.

Blob properties:
* 'd' the blob of data.