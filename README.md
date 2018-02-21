# Gbackup

***Note: This project is currently in the experiment phase. I'm trying out 
some ideas and maybe it'll turn into something useful, maybe not. But for 
now, this is not a working backup solution.***

Gbackup is a personal file backup solution that has the following main goals:

* Low runtime memory usage
* Fast and efficient filesystem scans to discover changed files
* Content-addressable storage backend (loosely based on Git's object format, 
hence Gbackup)

Other goals that are a priority for me:

* No special software needed on remote storage server (plan to target 
Backblaze B2)
* Client side encryption (plan to incorporate libsodium)
* Runs as a daemon with built-in scheduler (no cobbling together wrapper 
scripts and cron)
* Continuous file monitoring with inotify (this goal was originally intended to 
avoid having to do expensive filesystem scans, but I've since made the scanning 
quite efficient, so this may not be necessary)
* Use asymmetric key encryption to allow backup and prune operations 
without the secret key. Restore operations will require the secret key. (See 
below about encryption)

No other backup programs I've found quite met these criteria. Gbackup takes 
ideas from Borg, Duplicati, Bup, and others.

GBackup runs on Linux using Python 3.5.3 or newer. At the moment, 
compatability with any other platforms is coincidental.

These features are not a priority at the moment

* Multi-client support (multiple machines backing up to the same repository, 
with deduplication across all files. This would require repository locking 
and synchronizing of metadata, which isn't a problem I want to tackle right 
now)

## Architecture

### Terminology

Repository - The remote storage service where backup data is stored, 
    typically encrypted.
    
Backup set - The set of files on a local filesystem to be backed up. This is 
    defined by a single path to a root directory.
    
Snapshot - When a backup set is backed up, that forms a snapshot of all the 
    files in the backup set.

### Scan process

One of the fundamental aspects of any backup software is deciding which files
to back up and which files haven't changed since last backup. Some software, 
e.g. rsync, don't keep any local state and check which files have changed by 
comparing file attributes against a remote copy of the file. Some software 
keep a local database either in memory or on disk of file attributes and
compared the database values with the file. This is all to avoid transferring
more data across a perhaps slow network connection than is necessary, but 
with very large filesystems to back up, it's a necessary optimization.
 
GBackup can't read remote data, since that data may be encrypted with a 
private key that the backup process won't have access to. So our only option 
is to keep a local cache of file attributes of every file in the backup set. 
Early experiments used hierarchy of Python objects in memory. When the 
process started, the filesystem was traversed and the hierarchy of objects 
created, each object storing file attributes. Periodically, a simple 
recursive algorithm could traverse this tree in a post-order to iterate over 
all filesystem entries to either see if they've changed, or iterate over only
changed objects to back them up. Additionally, this architecture has a nice 
Python implementation using recursive generator functions and Python's `yield 
from` expressions. Each object's backup() function would yield objects to 
back up, and recurse using `yield from` into its child objects before backing
itself up.

This approach wasn't very fast and required quite a lot of memory to run. So 
I decided to experiment with the other extreme: store nothing in memory and
put all state into a local SQLite database. This turns out to have a lot of 
other advantages, in that I can do SQL queries to select and sort entries 
however I want, and gives more flexability in how I traverse the filesystem. 
But more importantly, it turns out to be very fast, with low memory usage. 
Scans are bounded mostly by the time to perform the lstat system call on 
every file in the backup set.

The current scan process is a multi-pass scan. On the first pass, it 
iterates over all objects in the local database (in arbitrary order) and 
performs an lstat to see if the entry has changed from what's in the database.
If it has changed, it's flagged as needing backup. For directories needing 
backup, a listdir call lists the children and any new entries are created for
new children. 

Subsequent passes perform the same operation on all newly created entries for
new files. Passes continue until there are no new files to scan.

This approach to scanning turns out to be very fast, especially for 
subsequent scans but even the inital scan. Memory usage remains low and if 
database writes are performed in a single SQLite transaction, IO is also kept
to a minimum. The reason scans are quick is it avoids problems with recursive 
tree traversals: each visit to a node would require a separate database query
or listdir call to get the list of children, which is more IO to perform. By 
scanning files in no particular order, every entry is streamed from the 
database in large batches and IO is kept to a minimum. The limiting factor is
having to perform all the lstat calls for every filesystem entry.

Note that a directory's mtime is updated by the creation or deletion of files
in the directory, so we can avoid listdir on unchanging directories. Also, if
the scan turns out to be CPU bound, it is easily parallelizable. However, 
raw speed is not the top priority, as consuming all of the CPU is not 
desirable for a program that runs in the background.

The backup process thus consists of iterating over all entries in the 
database that are flagged as needing backup, and backing them up.

### Storage Format

The storage repository is loosely based on Git's object store. With this, 
everything uploaded into the repository is an object, and objects are named 
after a hash of their contents. This has the advantage of inherent 
deduplication, as two objects with the same contents will only be stored once.

In this system, there are three kinds of objects: Tree objects, Inode 
objects, and Blob objects. They roughly correspond to their respective 
filesystem counterparts.

Tree objects store stat info about a directory, as well as a list of 
directory entries and the name of the objects for each entry. Directory 
entries can be other tree objects or inode objects.

Inode objects store stat info about a file, and a list of blob objects that 
make up the content of that file.

Blob objects store a blob of data.

Since each object is named with a hash of its contents, and objects reference
other objects by name, this forms a merkle tree. A root object's hash 
cryptographicaly verifies the entire object tree for a snapshot (much like Git).
If another snapshot is taken and only one file changed deep in the filesystem, 
then pushed to the repository are objects for the new file, as well as new 
objects for all parent directories up to the root.

Note that in this heirarchy of objects, objects may be referenced more than 
once—they may have more than one parent. A blob may be referenced by more 
than one inode (or several times in the same file), but also inode and tree 
objects may be referenced by more than one snapshot.

### Chunking

When backing up a file, the file's contents is split into chunks and each 
chunk is uploaded individually as its own blob. The algorithm for how to 
chunk the file will determine how good the deduplication is. Larger chunks 
mean it's less likely to match content elsewhere, while smaller chunks mean 
more uploads and more network overhead and slower uploads.

Some backup systems (such as Borg) use variable sized chunks and a rolling 
hash to determine where to split the chunk boundaries. This has the advantage
of synchronizing chunk boundaries to the content. Consider a fixed size chunk
of 4MB. A large file that doesn't change will use the same set of blob objects 
every time. But what if a single byte is inserted at the beginning of the 
file, pushing all the rest down one byte. Now suddenly the chunks don't match
previously uploaded ones, so the entire file is re-uploaded.

With a rolling hash over the last 4095 bytes like Borg uses, as files are 
scanned, the decision to split a file is based on the last 4095 bytes seen.
If one file is split at a particular location, and another file has the same 
4095 byte sequence somewhere in it, then there will be a chunk split there,
no matter where those 4095 bytes fall in the file.

This self-synchronization helps a lot to deduplicate large files whose 
contents is moving around. However, I believe this is rather rare. Consider 
most files in an average home directory of a personal computer fall into 
these categories:

* Text files
* Compressed binary documents (images, docx, xlsx)

Text files are typically small such that re-uploading the entire file on 
change won't be much overhead.
 
For large binary files, typically the format is such that the application 
does its own management of data, and won't involve shifting large amounts of 
data due to inserts or deletes in the file. The logic being that an 
application managing a large binary format won't want to do a lot of data 
moving or copying for efficiency reasons. So changes to the file aren't 
likely to produce similarities at different positions in the file.
 
Notable exceptions may include video files for video editing work, and virtual 
machine images.

My conclusion is that below about 30MB [1] it's probably not worth splitting 
files into more than one chunk. Further, a vast majority of files in my own home 
directory are less than 1 MB: about 97% out of about a million files. So
for now I don't believe using a rolling hash provides much practical
benefit, although it should be easy to substitute the chunking algorithm at a
later point.

[1] 30MB is the threshold Backblaze uses, below which files aren't chunked.
https://help.backblaze.com/hc/en-us/articles/217666728-How-does-Backblaze-handle-large-files-

### Object Cache

In the local database, a cache of objects is kept in a table. This table 
helps keep track of objects that have been uploaded to the remote repository,
saving network requests. This also lets us avoid uploading a file even if the
scanning process thinks a file changed when it hasn't. The file will be split
into chunks, the chunks hashed, and then the hash looked up in the database.
If the object already exists in the database, then it's assumed to have been 
uploaded to the repository already.

The Object cache also keeps track of relationships between objects. This is 
used when removing an old snapshot. When a snapshot is removed, the 
objects aren't immediately deleted, since they may be referenced by other 
snapshots. Instead, a garbage collection routine is used to traverse the 
object tree starting at each root, and calculate a set of unreachable objects.
Those objects are then deleted from the local cache and the remote repository.

### Pack files

### Backup process

### Encryption

