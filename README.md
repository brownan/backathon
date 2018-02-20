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

## Architecture

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
every file on the filesytem.

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

### Backup process

### Encryption

