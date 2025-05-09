from __future__ import annotations

import datetime
import itertools
import logging
import os
import pathlib
import sqlite3
from contextlib import contextmanager
from operator import itemgetter
from os import PathLike
from typing import Any
from typing import Generator
from typing import Iterator
from typing import Type
from typing import TypeVar
from typing import cast

from pydantic import BaseModel

from backathon import models
from backathon.exceptions import FSEntryNotFound
from backathon.settings import Settings
from backathon.signals import ConfigChange
from backathon.signals import SignalBus

logger = logging.getLogger("backathon.db")
sql_logger = logging.getLogger("backathon.db.sql")
sql_logger.setLevel(logging.INFO)

# Explicit datetime adapter. The default adapter is deprecated in Python 3.12
# Note: Converting back to Python types is done by Pydantic, not sqlite3 converters.
sqlite3.register_adapter(datetime.datetime, lambda val: val.isoformat())

M = TypeVar("M", bound=BaseModel)

MIGRATIONS: list[list[str]] = [
    [
        """CREATE TABLE objects (
            objid BLOB PRIMARY KEY,
            type TEXT NOT NULL,
            uploaded_size INTEGER,
            file_size INTEGER,
            last_modified_time TEXT,
            sha1 BLOB
        )""",
        """CREATE TABLE object_relations (
            parent BLOB NOT NULL REFERENCES objects (objid) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED,
            child BLOB NOT NULL REFERENCES objects (objid) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED,
            name BLOB DEFAULT null
        )""",
        """CREATE UNIQUE INDEX object_relations_unique
            ON object_relations
            (parent, child, name)
        """,
        """CREATE TABLE fsentry (
            id INTEGER PRIMARY KEY,
            objid BLOB REFERENCES objects (objid) ON DELETE SET NULL DEFERRABLE INITIALLY DEFERRED,
            path BLOB UNIQUE NOT NULL,
            parent INTEGER REFERENCES fsentry (id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED,
            new BOOLEAN DEFAULT TRUE,
            st_mode INTEGER,
            st_mtime_ns INTEGER,
            st_size INTEGER
        )""",
        """CREATE INDEX fsentry_new ON fsentry(new)""",
        """CREATE INDEX fsentry_parent ON fsentry(parent)""",
        """CREATE TABLE snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            path TEXT NOT NULL,
            root BLOB NOT NULL REFERENCES objects (objid) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
            timestamp TEXT
        )""",
        """CREATE TABLE garbage (
            objid BLOB PRIMARY KEY ON CONFLICT IGNORE
        )""",
    ]
]


def batch_fetch_from_cursor(cursor: sqlite3.Cursor):
    cursor.arraysize = 2048
    while batch := cursor.fetchmany():
        yield from batch


class CursorWrapper:
    def __init__(self, c: sqlite3.Cursor):
        self.cursor = c

    def execute(self, sql, param_list=()):
        sql_logger.debug(
            "Executing sql on conn %s:\n%s\n%r",
            id(self.cursor.connection),
            (sql),
            param_list,
            stacklevel=2,
        )
        return self.cursor.execute(sql, param_list)

    def executemany(self, sql, param_list=()):
        sql_logger.debug(
            "Executemany on conn %s:\n%s\n%r",
            id(self.cursor.connection),
            (sql),
            param_list,
            stacklevel=2,
        )
        return self.cursor.executemany(sql, param_list)

    def __getattr__(self, item):
        return getattr(self.cursor, item)


class Database:
    def __init__(
        self,
        path: str | PathLike[str],
        *,
        signal_bus: SignalBus | None = None,
        create: bool = False,
        initial_settings: Settings | None = None,
    ):
        self.signal_bus = signal_bus
        self._savepoint_num: int = 1

        self.path = pathlib.Path(path)
        if not create and not self.path.is_file():
            raise FileNotFoundError(f"Config database not found: {self.path}")
        elif create and self.path.is_file() and self.path.stat().st_size != 0:
            raise FileExistsError(f"Config database already exists: {self.path}")
        self.conn = self._open_db()
        self._setup_db()
        try:
            if create:
                assert initial_settings
                self.config = initial_settings
                self._config_set("settings", initial_settings.model_dump_json())
            else:
                existing_config = self._config_get("settings")
                if existing_config is None:
                    raise ValueError("No settings found")
                self.config = Settings.model_validate_json(existing_config)
        except Exception:
            if create:
                self.path.unlink()
            raise

    def save_config(self):
        serialized = self.config.model_dump_json()
        self._config_set("settings", serialized)
        if self.signal_bus is not None:
            for attr in self.config.changed_attrs:
                self.signal_bus.send(ConfigChange(key=attr))
        self.config.clear_changed_attrs()

    def clone(self) -> Database:
        """Returns a new database instance with a separate, isolated
        connection

        """
        # Since this is typically used to open a new connection in a separate
        # thread, create this with no signal bus, since the signal bus
        # isn't thread safe.
        return Database(self.path, signal_bus=None, create=False)

    def _open_db(self) -> sqlite3.Connection:
        logger.debug("Opening database %s", self.path)
        conn = sqlite3.connect(self.path, isolation_level=None)
        cursor = conn.cursor()
        cursor.execute("PRAGMA page_size=4096")
        cursor.execute("PRAGMA cache_size=-2000")
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA wal_autocheckpoint=1000")
        cursor.execute("PRAGMA journal_size_limit=10000000")
        cursor.execute("PRAGMA synchronous=NORMAL")
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.execute("PRAGMA auto_vacuum=INCREMENTAL")
        if os.cpu_count() or 0 > 2:
            cursor.execute("PRAGMA threads=2")
        cursor.execute("PRAGMA optimize=0x10002")
        cursor.close()
        conn.row_factory = sqlite3.Row
        return conn

    def _setup_db(self):
        cursor = self.conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS config (key TEXT UNIQUE ON CONFLICT REPLACE, value)
            """
        )

        current_migration_level: int | None = self._config_get("migration")

        migration_iter = enumerate(MIGRATIONS)
        if current_migration_level is None:
            migrations_to_run = migration_iter
        else:
            migrations_to_run = itertools.islice(
                migration_iter, current_migration_level + 1, None
            )

        for migration_num, migration in migrations_to_run:
            logger.info("Running migration #%s", migration_num)
            with self.atomic():
                for statement in migration:
                    logger.debug("Executing %s", statement.strip())
                    cursor.execute(statement)
                self._config_set("migration", migration_num)

        cursor.close()

    def close(self):
        self.conn.close()

    @contextmanager
    def cursor(self, *, retdict: bool = False) -> Iterator[sqlite3.Cursor]:
        c = self.conn.cursor()
        if retdict:
            c.row_factory = lambda c, r: dict(zip(map(itemgetter(0), c.description), r))
        if sql_logger.isEnabledFor(logging.DEBUG):
            c = cast(sqlite3.Cursor, CursorWrapper(c))
        try:
            yield c
        finally:
            c.close()

    def query(
        self, model_cls: Type[M], query: str, args: tuple[Any, ...] = ()
    ) -> Generator[M, None, None]:
        """Queries the database and returns instances of the given model

        Queries must make sure to return rows from the table corresponding to
        the given model class.

        Queries must make sure they retrieve columns covering at least the required
        fields of the model class. Usually queries should just select *.

        This method takes care of batching the calls to sqlite and passing the
        rows into model_validate(). It is the preferred way of retrieving
        FSEntry and Object instances from the database.
        """
        with self.cursor(retdict=True) as cursor:
            cursor.execute(query, args)
            yield from map(model_cls.model_validate, batch_fetch_from_cursor(cursor))

    def _config_get(self, key: str, default: Any = None) -> Any:
        cursor = self.conn.cursor()
        cursor.execute("SELECT value FROM config WHERE key = ?", (key,))
        result = cursor.fetchone()
        cursor.close()
        if result is None:
            return default
        return result[0]

    def _config_set(self, key: str, value: Any):
        cursor = self.conn.cursor()
        cursor.execute("INSERT INTO config (key, value) VALUES (?, ?)", (key, value))
        cursor.close()

    @contextmanager
    def atomic(self, *, immediate: bool = False):
        """Opens a database transaction and commits / rolls back when the context exits"""
        if self.conn.in_transaction:
            raise RuntimeError(
                "Cannot open an atomic block when already in a transaction"
            )
        if immediate:
            self.conn.execute("BEGIN IMMEDIATE")
        else:
            self.conn.execute("BEGIN")

        # Using a sqlite3 connection as a context manager will close the transaction or
        # roll it back like we want
        with self.conn:
            yield

    @contextmanager
    def savepoint(self):
        """Creates a database savepoint and releases it when the context exits"""
        if not self.conn.in_transaction:
            raise RuntimeError("Cannot create a savepoint outside of a transaction block")

        savepoint_name = f"savepoint{self._savepoint_num}"
        self._savepoint_num += 1
        self.conn.execute(f"SAVEPOINT {savepoint_name}")
        try:
            yield
        except BaseException:
            self.conn.execute(f"ROLLBACK TO {savepoint_name}")
            raise
        finally:
            self.conn.execute(f"RELEASE {savepoint_name}")

    @contextmanager
    def atomic_or_savepoint(self):
        """Opens a transaction or creates a savepoint depending on whether a transaction
        is already open

        This function should only be used in special situations where a function is
        designed to be used in different contexts. Most places should prefer to use
        atomic() or savepoint() as appropriate in order to tightly control when
        transactions and savepoints are committed and rolled back.
        """
        if not self.conn.in_transaction:
            with self.atomic():
                yield
        else:
            with self.savepoint():
                yield

    def get_fsentry(self, path: str | bytes | os.PathLike) -> models.FSEntry:
        """Shortcut to get an fsentry by its path, or raise a FileNotFound exception"""
        path = os.fspath(path)
        if isinstance(path, str):
            path = os.fsencode(path)
        entry = next(
            self.query(
                models.FSEntry,
                "SELECT * FROM fsentry WHERE path=? LIMIT 1",
                (path,),
            ),
            None,
        )
        if entry is None:
            raise FSEntryNotFound(path)
        return entry
