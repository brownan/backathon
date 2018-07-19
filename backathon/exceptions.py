class CorruptedRepository(Exception):
    """Raised when an inconsistency was detected while reading from a storage
    repository

    These errors indicate something was wrong with remote data. This could be
    due to a bug or perhaps could indicate someone has manually messed with
    some files. In either case, it may not be recoverable.
    """
    pass


class DependencyError(Exception):
    pass