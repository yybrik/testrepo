"""Exclusive process ownership plus SQLite durable transactions."""
import hashlib
import json
import os
import sqlite3
import uuid
from dataclasses import asdict
from pathlib import Path

from .crypto import canonical


def configuration_hash(config):
    return hashlib.sha256(canonical(asdict(config))).hexdigest()


class ProcessLease:
    def __init__(self, database):
        self.file = open(str(database) + ".lock", "a+b")
        self.file.seek(0)
        self.file.write(b"0")
        self.file.flush()
        self.file.seek(0)
        try:
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(self.file.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(self.file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            self.file.close()
            raise RuntimeError("Another gate owns this database") from None

    def close(self):
        if self.file.closed:
            return
        if os.name == "nt":
            import msvcrt
            self.file.seek(0)
            msvcrt.locking(self.file.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl
            fcntl.flock(self.file.fileno(), fcntl.LOCK_UN)
        self.file.close()


def connect(database):
    # mode=rw forbids accidental creation following data loss.
    db = sqlite3.connect(Path(database).resolve().as_uri() + "?mode=rw",
                         uri=True, isolation_level=None, check_same_thread=False)
    # WAL lets read-only exports run without blocking gate commits.
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=FULL")
    db.execute("PRAGMA busy_timeout=3000")
    return db


def initialize(database, config, now):
    path = Path(database)
    path.parent.mkdir(parents=True, exist_ok=True)
    lease = ProcessLease(path)
    try:
        path.touch(exist_ok=False)
        db = connect(path)
        try:
            db.executescript("""
                BEGIN IMMEDIATE;
                CREATE TABLE state (id INTEGER PRIMARY KEY CHECK(id=1), value TEXT NOT NULL);
                CREATE TABLE metadata (id INTEGER PRIMARY KEY CHECK(id=1), version INTEGER,
                                       configuration_hash TEXT);
                CREATE TABLE requests (request_id TEXT PRIMARY KEY, fingerprint TEXT NOT NULL,
                                       result TEXT NOT NULL);
                CREATE TABLE events (event_id INTEGER PRIMARY KEY AUTOINCREMENT, value TEXT NOT NULL);
            """)
            initial = dict(epoch=-1, mode="UNINITIALIZED", holder="", revision=0,
                           lane_state="UNKNOWN", grant=None, session_id=str(uuid.uuid4()),
                           observation_sequence=0, observation_revision=0, last_observation_at=None,
                           observation_not_before=now,
                           last_clock=now, safety_stop_since=now,
                           safety_stop_total_seconds=0.0)
            db.execute("INSERT INTO state VALUES(1,?)", (canonical(initial).decode(),))
            db.execute("INSERT INTO metadata VALUES(1,2,?)", (configuration_hash(config),))
            db.commit()
        finally:
            db.close()
    finally:
        lease.close()


def load_state(db):
    return json.loads(db.execute("SELECT value FROM state WHERE id=1").fetchone()[0])


def save_state(db, state):
    db.execute("UPDATE state SET value=? WHERE id=1", (canonical(state).decode(),))
