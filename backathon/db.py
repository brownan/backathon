import itertools
import json
import logging
import os
import pathlib
import sqlite3
from contextlib import contextmanager
from operator import itemgetter
from os import PathLike
from typing import (
    Any,
    Generator,
    Iterator,
    Type,
    TypeVar,
)

from pydantic import BaseModel

logger = logging.getLogger("backathon.db")

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
            name TEXT DEFAULT null
        )""",
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
            path BLOB NOT NULL,
            root BLOB NOT NULL REFERENCES objects (objid) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
            date TEXT
        )""",
        """CREATE INDEX snapshots_date ON snapshots(date)""",
    ]
]


class Database:
    def __init__(self, path: PathLike):
        self.path = pathlib.Path(path)
        self.conn = self._open_db()
        self._setup_db()
        self._savepoint_num: int = 1

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

        current_migration_level: int | None = self.config_get("migration")
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
                self.config_set("migration", migration_num)

        cursor.close()

    @contextmanager
    def cursor(self, *, retdict: bool = False) -> Iterator[sqlite3.Cursor]:
        c = self.conn.cursor()
        if retdict:
            c.row_factory = lambda c, r: dict(zip(map(itemgetter(0), c.description), r))
        try:
            yield c
        finally:
            c.close()

    def get_objects(
        self, model_cls: Type[M], query: str, args: tuple[Any, ...] = ()
    ) -> Generator[M, None, None]:
        with self.cursor(retdict=True) as cursor:
            cursor.arraysize = 2048
            cursor.execute(query, args)
            while True:
                rows = cursor.fetchmany()
                if not rows:
                    break
                yield from map(model_cls.model_validate, rows)

    def config_get(self, key: str, default: Any = None) -> Any:
        cursor = self.conn.cursor()
        cursor.execute("SELECT value FROM config WHERE key = ?", (key,))
        result = cursor.fetchone()
        cursor.close()
        if result is None:
            return default
        return result[0]

    def config_set(self, key: str, value: Any):
        cursor = self.conn.cursor()
        cursor.execute("INSERT INTO config (key, value) VALUES (?, ?)", (key, value))
        cursor.close()

    def config_get_json(self, key: str, default: Any = None) -> Any:
        data = self.config_get(key, None)
        if data is None:
            return default
        return json.loads(data)

    def config_set_json(self, key: str, value: Any):
        self.config_set(key, json.dumps(value))

    @contextmanager
    def atomic(self, *, immediate: bool = False):
        if not self.conn.in_transaction:
            if immediate:
                self.conn.execute("BEGIN IMMEDIATE")
            else:
                self.conn.execute("BEGIN")
            with self.conn:
                yield

        else:
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
