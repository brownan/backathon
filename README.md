# Backathon

***Note: This project is currently in the experiment phase. I'm trying out 
some ideas and maybe it'll turn into something useful, maybe not. But for 
now I wouldn't recommend using unless you're interested in development or 
contributing ideas. Use at your own risk!***

Backathon is a personal file backup solution that has the following key selling
points:

* Runs as a daemon with built-in scheduler (no cobbling together wrapper 
  scripts and cron)
* Use asymmetric encryption to allow unattended backup and prune operations 
  without a password. Only restore operations will require the password
* Repository format is a content-addressable object store for deduplication 
  and quick restores from any past snapshot (loosely based on Git's object 
  format)

Additionally, these are the main design goals that are a priority for me:

* Low runtime memory usage: There are no in-memory data structures whose
  size depends on the size of the backup set or repository, so memory usage
  doesn't grow with the size of your backups.
* Fast and efficient filesystem scans to discover changed files. Scans and
  backups where not much has changed are very cheap.
* Decoupled scan and backup routines. This allows the future implementation of
  "continuous" style backups with e.g. inotify, where the scan routine runs
  more often or continuously, and the backup routine happens less often (e.g.
  15 minutes).
* Write-only backups: the backup operation reads nothing from the repository,
  allowing more secure setups with write-only api keys to the storage
  provider. This sacrifices pruning and restores for better security against
  a compromised client.
* The local cache uses SQLite which keeps memory low (cache data is on disk and
  not in memory) while still allowing fast, indexed access to cache data.
  Operations like browsing backed up file manifests are quick and allow
  restores of select files or entire snapshots.
* Fast and efficient pruning of old backups to recover space by calculating
  garbage solely client side using the local cache database.
* Targets any generic object storage service (Currently supporting and
  optimized for Backblaze B2)
* Optimize repository access: many transaction types in Backblaze B2 cost
  money. Backathon should do as much as it can locally using the cache
  database and avoid expensive repository operations where possible. Of
  course a complete disaster recovery still requires reading in the entire
  repository, but common operations should be cheap or entirely client-side.
* Client side encryption using libsodium
* Keep the code and architecture simple. Complexity is avoided except when
  absolutely necessary

No other backup programs I've found quite met these criteria. Backathon takes 
ideas from Borg, Restic, Duplicati, and others.

Backathon runs on Linux using Python 3.5.3 or newer. At the moment, 
compatability with any other platforms is coincidental.

These features are not a priority and probably won't be implemented any time
soon:

* Multi-client support, meaning multiple machines backing up to the same
  repository, with deduplication across all files. This would require
  repository locking (or reworking of the prune routine), synchronizing of
  metadata (or expensive repository scans each backup), and would greatly
  complicate the encryption (or require the same encryption keys on each
  client). These aren't problems I want to tackle right now since I don't
  consider multi-client repositories that important. Space is cheap and my
  computers don't share that much data between them.
  
  Omitting multi-client support is my compromise for the set of features
  I want in a backup system.
  
  Note that I do plan on having read-only clients to allow restores from
  other machines. However, curretly Backathon assumes its local cache database
  is authorative.
  
## Roadmap
What's done and not done?

Main TODO list:
* Scanning routine: Working
* Backup routine: Working
* Local filesystem storage backend: Working
* Encryption functionality: Working
* Compression functionality: Working
* Restore routine: Working
* Command line interface: Working
* B2 Backend: Needs Testing
* Prune routine: Partial
* Verify routine: Not started
* Scheduler / Daemon: Not started
* GUI: Not started
* Inotify integration: Not started
* Multi-client readers: Not started

Components with status "Working" means that component is working in at
least a minimal capacity, but there still may be work left to do.
"Partial" means some work has been done but the component is not working yet.

In addition to the above tasks, this is the status of the other components:
* The database schema is not yet finalized and may change
* The object format is not yet finalized and may change

## Architecture

### Terminology

Repository - The remote storage service where backup data is stored, 
    typically encrypted.
    
Backup set - The set of files on a local filesystem to be backed up.
    
Snapshot - When a backup set is backed up, that forms a snapshot of all the 
    files in the backup set at that point in time. Snapshots are saved in the
    repository and are available for later restore.

### Scan process and files cache

Before a backup can be made, the backup set must be scanned. The scan
determines which files have changed and therefore which files need backing 
up. In many backup programs these two functions happen together: files are 
scanned and backed up if needed in a single step. However, in Backathon the scan 
routine is decoupled from the backup routine. This has several advantages:

* The scanning and backup routines can be tuned and optimized independently,
simplifying the code.
* Scanning can be supplemented with an inotify watcher. Full scans can
then happen much less often (e.g. once a day) while inotify runs continuously
in the background and marks files as "dirty" when they change. The backup
routine then only has to read in those files.
* Having a list of "dirty" files lets us report accurate info on the size
and number of files to be backed up, as well as accurate progress bars during
backup.

Backathon keeps a local cache of all files in the backup set, and stores some 
metadata on each one. When a scan is performed, metadata from an `lstat()` 
system call is compared with the information in the cache, and if the 
information differs, the file is marked as dirty and will be backed up next 
backup.

(Note that a file marked as dirty doesn't necessarily mean its contents have 
changed. During the backup, the file's contents is read in and hashed to 
determine if any new chunks actually need uploading. The scan really just 
finds which files should be read in and checked.)

Right now, the metadata stored and used to determine changed files is:

* `st_mode` (includes file type and permissions)
* `st_mtime_ns` (last modified time)
* `st_size`

The file cache is kept in a local SQLite table. The scan process selects all 
entries from this table and iterates over them, performing the `lstat()` call
on each one. If a file has changed according to the metadata listed above, it
is marked as dirty and its metadata updated in the database. If a directory 
has changed, a `listdir()` is performed and its children updated: any old 
children are deleted and any new children are added and flagged as "new". The
table is iterated over until there are no "new" entries left.
 
Traversing the entries by iterating over the database table helps keep I/O 
relatively low compared to traversing the filesystem, which would require a 
`listdir()` call to each directory. We avoid `listdir()` calls on directories
that haven't changed by comparing the metadata: when a directory's entries 
change its mtime is updated. 

This results in very fast scans over files that haven't changed. 
The initial scan is slower, mostly due to the I/O in performing all the 
inserts into the cache table, but there may be room for optimization here. 
Memory is also kept low since all data is stored on disk in the SQLite database.

### Storage Format

The storage repository is loosely based on Git's object store: 
everything uploaded into the repository is an object, and objects are 
identified by a hash of their contents. This has the advantage of inherent 
deduplication, as two objects with the same contents will only be stored once.

In this system, there are three main types of objects: Tree objects, Inode
objects, and Blob objects. They roughly correspond to their respective 
filesystem counterparts.

Tree objects store stat info about a directory, as well as a list of
directory entries and the name of the objects for each entry. Directory
entries can be other tree objects or inode objects.

Inode objects store stat info about a file, and a list of blob objects that 
make up the content of that file.

Blob objects store a blob of data.

Since each object is identified by a hash of its contents, and objects 
reference other objects by their identifier, this forms a merkle tree. A root
object's hash cryptographicaly verifies the entire object tree for a snapshot
(much like Git). If another snapshot is taken and only one file changed deep 
in the filesystem, then pushed to the repository are objects for the new 
file, as well as new objects for all parent directories up to the root. All 
identical objects are shared between snapshots.

### Object Cache and Garbage Collection

In the local SQLite database, along with the filesystem cache table, there is
an object cache table. This table keeps track of objects that exist in the 
remote storage repository. This allows Backathon to avoid uploading objects 
that already exist by performing a quick query to the local database.

Since objects hold references to each other, another local table of object 
relationships is maintained. This forms a directed graph of objects, and 
allows Backathon to calculate which objects should be deleted when an old 
snapshot is pruned. Since objects may be referenced by more than one 
snapshot, only objects not reachable by any snapshot may be deleted.

This is a classic garbage collection problem. To calculate the set of 
"garbage" objects quickly and efficiently, there are several options 
available including classic and well researched garbage collection algorithms. 
I've however chosen to implement a bloom filter, tuned to collect on average 
95% of all garbage objects. This has the advantage of taking just two passes 
over the entire object collection, the first of which is read only, so there 
is low I/O. And since it's tuned for 95% (instead of something higher) the 
memory usage is also low: about 760k for a million objects.

The first pass walks the tree of objects starting at the snapshot roots. This
walks the set of reachable objects. Each object is added to the bloom filter.
The hash functions are simply randomly generated bit strings xor'd into the
object identifiers, which themselves are already random strings since 
they're cryptographic hashes.

The second pass iterates over the entire table of objects. If an object 
appears in the bloom filter, there's a 95% change it was reachable. If the 
item doesn't appear, then the bloom filter guarantees the object was not 
reachable and can be deleted.

Since the garbage collection calculations happen entirely on the client-side,
the client can issue delete requests for objects in the remote repository 
without having to download and decrypt them. This keeps with the goal of not 
needing the encryption password for routine prune operations.

### Chunking

When backing up a file, the file's contents is split into chunks and each
chunk is uploaded individually as its own blob object. The chunking algorithm
determines where and how many splits to make, which also determines how good
the deduplication is. Larger chunks make for more efficient uploads, but
mean it's less likely to match content elsewhere (since a single change
in a chunk will cause the entire chunk to be re-uploaded). Smaller
chunks give better deduplication, but mean more uploads, more network
overhead, slower uploads, and more cache overhead.

Right now Backathon uses a fixed size chunking algorithm: files are simply 
split every fixed number of bytes. Fixed size chunkers are quick and simple 
but don't provide good deduplication between files if it's unlikely similar 
regions will align to the same chunk boundaries, or between the same file 
across snapshots in cases where bytes are inserted into files pushing existing 
data down and causing chunk misalignment.

The fixed size chunker is likely to change to something more sophisticated in
the future, but I believe fixed size chunking is adequate for most kinds of 
files found in a typical desktop user's home directory. My rationale is that 
most files are going to be very small (so deduplication won't help much), or 
are going to be binary or compressed file formats that will be completely 
rewritten on change, and probably won't benefit much from deduplication at 
all. Most large binary formats would want to avoid inserting bytes because 
that would involve copying large amounts of data to other sections of the file.

So large files that manage their data effectively will deduplicate effectively 
with a fixed size chunk. And small files are small enough to just upload 
completely each change.

This leaves two questions:

1. How big should the chunks be?
2. How large does a file have to be before it's worth chunking at all?

Backblaze has set these parameters at 10MB and 30MB respectively [1] for their
personal backup service, and seem like good initial values to me. As the
software matures and I get more feedback and benchmarks, these parameters can
be tuned.

Some backup systems (such as Borg) use variable sized chunks and a rolling 
hash to determine where to split the chunk boundaries. This has the advantage
of synchronizing chunk boundaries to the content, so a single inserted byte 
won't cause the chunk boundaries to misalign with the previous backup causing
the entire file to be re-uploaded. It's also more likely to discover similar 
portions within a file and across different files. This increases the 
deduplication in lots of situations where fixed size chunking falls flat. 
Some examples where fixed size chunking is likely to perform poorly:
 
* virtual machine images, which may have lots of duplicate data throughout 
but not necessarily aligned to chunk boundaries.
* SQL database dumps. Each database dump will contain lots of identical data,
 but not necessarily in the same places within the file.
* Video files for video editing. Changes in one section of a video 
may change the alignment of the rendered video but content in other sections 
stays the same.
 
Something of this sort is likely to be implemented in the future but is lower
on my priorities for the reasons explained above. I want to optimize for the 
common case, and as a single data point: 97% of the million files in my home 
directory are below 1MB, and probably aren't worth chunking at all.

[1] https://help.backblaze.com/hc/en-us/articles/217666728-How-does-Backblaze-handle-large-files-

### Backup process

*TODO*

### Threat Model

***Note: encryption is not yet fully implemented. Below is an outline of my 
plans, which are still shifting as I learn more and compare strategies from 
existing projects***

Backathon uses encryption, like many backup programs, to protect your data 
repository. With Backathon, the threat model is an adversary with access to the
repository (read or write) while assuming a secure and trusted client. The 
goal is to prevent leaking as much information as possible to adversaries 
with read access, and detect modifications made by adversaries with write 
access.

Specifically, Backathon's encryption has these properties:

* All backed up file data and metadata is encrypted and authenticated, making 
recovering plain text files, metadata, or directory structures impossible 
without the encryption keys or password
* Modifications to valid objects are detected by using encryption
algorithms that incorporate authentication
* Object identifiers are an HMAC of their plaintext contents, revealing no 
information and also providing another layer of authentication for objects
* Attacker-created objects inserted into the repository are rejected due to not 
being encrypted and authenticated with the proper keys
* Valid, deleted objects may be re-inserted (replay attack), but it's 
impossible for an attacker to construct a new original snapshot out of 
existing or old valid objects since the references to other objects within 
the object payloads are authenticated.

These are the possible threats with this model. Review this section carefully
and decide whether these threats are acceptable to you before using this
software.

* Since snapshots are stored one per file, an attacker knows how many 
snapshots exist
* An attacker can restore an old snapshot file. Without all the referenced 
objects, the snapshot would fail to restore entirely (although 
the local cache would have no knowledge of the snapshot, and rebuilding the 
cache from the repository would notice missing objects)
* An attacker can restore an old snapshot and all referenced objects, 
effectively restoring that snapshot
* An attacker can delete objects or corrupt their contents to render some or 
all snapshots inoperable. Such corruption would be detected during a restore,
but would not be detected during the normal backup process (as backups are a
write-only operation)
* An attacker observing access patterns can learn how often backups are 
taken, and how much data is written to the repository
* Careful analysis of the uploaded object sizes, number of objects at 
each size, and the pattern/ordering of uploaded objects may reveal some 
information about file sizes or directory structure of the backup set. For 
example, lots of small files, or lots of directories would generate more 
metadata objects, which have a fairly predictiable and consistent size.
* If an attacker has write access to a file in the backup set, it's possible 
to mount a fingerprinting attack, where known data is written to a local file.
The attacker can then observe whether a new object is uploaded to the
repository or not, revealing whether that chunk of data already existed in the
repository from some other file. This is a consequence of any deduplication
system, and although there may be ways to make this sort of attack more
difficult, it's unlikely to be eliminated entirely.

Another goal of Backathon is to not require a password for backup and other
write-only operations to the repository, as it's designed to run in the
background and start automatically at boot (unattended backups). The obvious
way to achieve this is with public/private key encryption. The public key is
used for encrypting files before uploading, and is stored in plain text
locally. Decryption requires the private key, which is stored encrypted with a
password.

This is the outline of how encryption is used:

1. When a repository is initialized, a password is entered. The password is 
used to derive a symmetric encryption key. The parameters used in the key 
derivation are saved locally and to the remote repository.
2. A public/private keypair is generated from high quality random sources. 
The password key is used to encrypt the private key. The encrypted private 
key is saved locally and to the remote repository as metadata.
3. The plain text public key is stored locally
4. During a backup, the public key is used to encrypt data before uploading 
to the repository. Object IDs are derived using HMAC-SHA256 using the public 
key as the HMAC key.
5. During a restore, the password is entered, the password key 
derived, and the private key decrypted. The private key is then used to 
decrypt downloaded data

(Deriving a key to encrypt a randomly generated private key lets us change 
the password without having to re-encrypt all encrypted data)

So a public/private key pair is used to encrypt all the data. The private key
is stored encrypted locally and in the repository, allowing restores with a
password. This protects against an adversary with access to the remote
repository.

The public key, however, is not really "public". While it's kept in plain text
locally to allow unattended backups, it must also be kept secret from
adversaries as it lets the holder create valid, authenticated objects in the
repository. (Storage credentials, API keys, etc are also stored unencrypted
locally)

The threat model therefore assumes the local machine is secure and
uncompromised. If the public key is compromised, there are more threats
possible since an attacker could create and upload valid objects into the
object store and perform brute force attacks on the object IDs to recover their
contents. I believe this is an acceptable compromise for unattended backups
since if your local filesystem is compromised, the bigger threat is the
attacker just reading your local files directly.

I'd like to note that a lot of other backup systems generate a symmetric key
for data encryption, encrypt that key with a password, and then leave password
management up to the user. If the user wishes to schedule unattended backups
e.g. from a cron script, they have to store the password in plaintext
somewhere. I consider that setup to have an equivalent amount of security as
just storing the whole key unencrypted.

So why bother with passwords at all if you assume a secure local machine?  Why
not just generate and store a symmetric key locally in plain text? A few
reasons:

1. While it's outside the threat model, the public/private encryption *is*
still protecting the repository data from being read if the public key is
compromised.
2. For consistency: if you derive a key from a password and store it 
unencrypted locally (and encrypted on the remote store), then you can perform
backup *and* restore operations without the password. But as soon as you lose
a hard drive and need to restore from scratch, you need the password. Some
restore operations need a password and some don't.
3. To prevent human error: along with the above, you're more likely to forget
your password if you've never needed it before a total system crash.
4. Any other scenarios where it's necessary to prevent read access to the 
repository even if read access to the local filesystem is possible 
(intentionally or unintentionally)

If protecting the repository from a compromised client is a priority, then
Backathon's write-only backups make it possible to configure a storage service
with write-only access for an API key. The client wouldn't be able to prune old
snapshots, and a different client would have to perform restores however. This
is currently an untested setup but should theoretically work.

Additionally, storing the local public key in an OS-managed keyring or hardware
key storage would also help against some local threats. This is not currently
implemented but theoretically possible.

### Encryption Algorithms

***Note: encryption is not yet fully implemented. Below is an outline of my 
plans, which are still shifting as I learn more and compare strategies from 
existing projects***

Backathon uses [libsodium](https://download.libsodium.org/doc/) for all encryption
operations via the [PyNaCl](https://pynacl.readthedocs.io) bindings to the
library.

A public-private key pair is generated at repository initialization time. The
public part of the key is stored in plain text locally. The private part is 
encrypted with a password and stored locally and in the remote repository.

To do this, libsodium's implementation of the
[Argon2id](https://download.libsodium.org/doc/password_hashing/the_argon2i_function.html)
key derivation function is used to generate a symmetric key from the password. 
The salt, opslimit, and memlimit paramaters are stored unencrypted both 
locally and in the remote repository. This symmetric key is then used to 
encrypt the private key with libsodium's
[Secret Box](https://download.libsodium.org/doc/secret-key_cryptography/authenticated_encryption.html)
which encrypts and authenticates using XSalsa20-Poly1305. This symmetric key 
is not stored anywhere. It is re-derived from the password if access to the 
private key is needed (e.g. for a restore operation)

During a backup, all objects are encrypted using the libsodium
[Sealed Box](https://download.libsodium.org/doc/public-key_cryptography/sealed_boxes.html)
construction, which is also implemented with XSalsa20-Poly1305.
The encryption key is derived using an X25519 key exchange between the 
user's public key and an ephemeral key generated for each object.

