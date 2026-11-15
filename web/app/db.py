"""SQLite persistence for the collection web service.

The database is shared by the two replicated web processes through a mounted
volume, so SQLite (WAL mode) plus ``BEGIN IMMEDIATE`` is the cross-process
concurrency boundary:

* only one RUNNING collection per (section, date) may exist;
* a late collection may never overwrite a published day whose source revision
  is newer (``StaleRevisionError``);
* publication replaces the whole day atomically -- partial pages never become
  visible;
* a review-team withdrawal removes the published observations of a contaminated
  day in one transaction and records why; the withdrawal acts as a *revision
  fence*: a collection carrying the same or an older revision can never
  re-publish, only a strictly higher revision that passed the full-day
  validation may restore availability (``WithdrawnRevisionError``);
* withdrawal uses optimistic revision checking: the client submits the
  revision it saw; if the day has since moved to another revision the request
  is rejected (``RevisionConflictError``) so a stale form can never withdraw a
  new archive;
* reads return withdrawal state and observations from a single read
  transaction, i.e. one committed snapshot.

The module deliberately depends only on the standard library so the demo
harness can exercise the guards with ``python -I`` in-process.
"""
from __future__ import annotations

import os
import sqlite3
from datetime import datetime, timezone
from typing import Optional

SCHEMA = """
CREATE TABLE IF NOT EXISTS published (
    section      TEXT NOT NULL,
    date         TEXT NOT NULL,
    revision     INTEGER NOT NULL,
    time         TEXT NOT NULL,
    nh3n         REAL NOT NULL,
    source_page  INTEGER NOT NULL,
    published_at TEXT NOT NULL,
    published_by TEXT NOT NULL,
    PRIMARY KEY (section, date, time)
);

CREATE TABLE IF NOT EXISTS runs (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    section        TEXT NOT NULL,
    date           TEXT NOT NULL,
    worker         TEXT NOT NULL,
    status         TEXT NOT NULL CHECK (status IN ('running','success','failed','interrupted')),
    revision       INTEGER,
    rows_collected INTEGER NOT NULL DEFAULT 0,
    page_attempts  INTEGER NOT NULL DEFAULT 0,
    error          TEXT,
    started_at     TEXT NOT NULL,
    finished_at    TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_runs_active
    ON runs (section, date) WHERE status = 'running';

-- One audit row per withdrawal. At most one *active* (not yet superseded by a
-- newer published revision) withdrawal may exist per section/day; the partial
-- unique index enforces this across both web processes.
CREATE TABLE IF NOT EXISTS withdrawals (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    section                 TEXT NOT NULL,
    date                    TEXT NOT NULL,
    revision                INTEGER NOT NULL,
    reason                  TEXT NOT NULL,
    withdrawn_at            TEXT NOT NULL,
    withdrawn_by            TEXT NOT NULL,
    superseded_at           TEXT,
    superseded_by_revision  INTEGER
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_withdrawals_active
    ON withdrawals (section, date) WHERE superseded_at IS NULL;
"""


class ActiveRunError(Exception):
    """Another collection for this section/day is still in progress."""


class StaleRevisionError(Exception):
    """A newer revision of the day is already published locally."""


class NoPublishedError(Exception):
    """Withdrawal requested for a day that has no published version."""


class AlreadyWithdrawnError(Exception):
    """The day is already withdrawn and has not been re-published yet."""


class RevisionConflictError(Exception):
    """Optimistic check failed: the day moved to another revision."""

    def __init__(self, expected_revision: int, current_revision: int) -> None:
        super().__init__(
            f"revision conflict: expected to withdraw revision "
            f"{expected_revision}, but the day now holds revision {current_revision}"
        )
        self.expected_revision = expected_revision
        self.current_revision = current_revision


class WithdrawnRevisionError(Exception):
    """Publish blocked by an active withdrawal fence for this day."""

    def __init__(self, revision: int, fence_revision: int) -> None:
        super().__init__(
            f"refusing to publish revision {revision}: revision {fence_revision} "
            f"or newer was withdrawn; only a strictly higher revision may restore "
            f"the day"
        )
        self.revision = revision
        self.fence_revision = fence_revision


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def get_conn(db_path: str) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
    conn = sqlite3.connect(db_path, isolation_level=None, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def rollback_safely(conn: sqlite3.Connection) -> None:
    """Roll back if (and only if) a transaction is still active.

    The guard paths below raise domain errors after an explicit rollback, so a
    blanket ``except: ROLLBACK`` would itself fail with "no transaction is
    active". Checking the connection's in-transaction state makes rollback
    idempotent and keeps every write path all-or-nothing.
    """
    if getattr(conn, "in_transaction", False):
        conn.execute("ROLLBACK")


def init_db(db_path: str) -> None:
    conn = get_conn(db_path)
    try:
        conn.executescript(SCHEMA)
    finally:
        conn.close()


def recover_interrupted(db_path: str) -> int:
    """Mark runs left 'running' by a crashed/restarted worker."""
    conn = get_conn(db_path)
    try:
        cur = conn.execute(
            "UPDATE runs SET status='interrupted', "
            "error='worker restarted before collection finished', finished_at=? "
            "WHERE status='running'",
            (utc_now(),),
        )
        return cur.rowcount
    finally:
        conn.close()


def start_run(db_path: str, section: str, date: str, worker: str) -> int:
    conn = get_conn(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            cur = conn.execute(
                "INSERT INTO runs (section, date, worker, status, started_at) "
                "VALUES (?, ?, ?, 'running', ?)",
                (section, date, worker, utc_now()),
            )
            conn.execute("COMMIT")
            return int(cur.lastrowid)
        except sqlite3.IntegrityError:
            rollback_safely(conn)
            raise ActiveRunError(
                f"a collection for {section} {date} is already running"
            )
    finally:
        conn.close()


def update_progress(
    db_path: str,
    run_id: int,
    revision: Optional[int],
    rows_collected: int,
    page_attempts: int,
) -> None:
    conn = get_conn(db_path)
    try:
        conn.execute(
            "UPDATE runs SET revision=?, rows_collected=?, page_attempts=? WHERE id=?",
            (revision, rows_collected, page_attempts, run_id),
        )
    finally:
        conn.close()


def fail_run(db_path: str, run_id: int, error: str) -> None:
    conn = get_conn(db_path)
    try:
        conn.execute(
            "UPDATE runs SET status='failed', error=?, finished_at=? WHERE id=?",
            (error[:500], utc_now(), run_id),
        )
    finally:
        conn.close()


def publish_version(
    db_path: str,
    section: str,
    date: str,
    revision: int,
    rows: list[dict],
    worker: str,
    run_id: int,
) -> dict:
    """Atomically replace the whole day's published version.

    Guards (all evaluated inside one ``BEGIN IMMEDIATE`` transaction):

    * an active withdrawal fences the day: ``revision`` must be strictly
      greater than the withdrawn revision, otherwise ``WithdrawnRevisionError``
      -- same/older in-flight collections can never re-publish a withdrawn
      day, even if they arrive late;
    * a strictly newer published revision wins over a late collector
      (``StaleRevisionError``); re-publishing the same revision is idempotent.

    When a higher revision is published while a withdrawal is active, the
    withdrawal is marked superseded in the same transaction. Any failure rolls
    the whole transaction back, leaving the previous state untouched.
    """
    conn = get_conn(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            fence_row = conn.execute(
                "SELECT MAX(revision) AS rev FROM withdrawals "
                "WHERE section=? AND date=? AND superseded_at IS NULL",
                (section, date),
            ).fetchone()
            fence = None if fence_row is None or fence_row["rev"] is None else int(fence_row["rev"])
            if fence is not None and revision <= fence:
                raise WithdrawnRevisionError(revision, fence)

            row = conn.execute(
                "SELECT revision FROM published WHERE section=? AND date=? LIMIT 1",
                (section, date),
            ).fetchone()
            current = None if row is None else int(row["revision"])
            if current is not None and current > revision:
                raise StaleRevisionError(
                    f"refusing to publish revision {revision}: revision {current} "
                    f"is already published for {section} {date}"
                )

            now = utc_now()
            conn.execute(
                "DELETE FROM published WHERE section=? AND date=?", (section, date)
            )
            conn.executemany(
                "INSERT INTO published (section, date, revision, time, nh3n, "
                "source_page, published_at, published_by) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    (
                        section,
                        date,
                        revision,
                        r["time"],
                        float(r["nh3n"]),
                        int(r["source_page"]),
                        now,
                        worker,
                    )
                    for r in rows
                ],
            )
            conn.execute(
                "UPDATE runs SET status='success', revision=?, rows_collected=?, "
                "finished_at=?, error=NULL WHERE id=?",
                (revision, len(rows), now, run_id),
            )
            if fence is not None:
                # A strictly higher revision restored the day: close the fence.
                conn.execute(
                    "UPDATE withdrawals SET superseded_at=?, superseded_by_revision=? "
                    "WHERE section=? AND date=? AND superseded_at IS NULL",
                    (now, revision, section, date),
                )
            conn.execute("COMMIT")
        except Exception:
            # Domain guards and storage failures alike roll back here; nothing
            # is ever partially applied, and the prior state stays intact.
            rollback_safely(conn)
            raise
        return {
            "action": "created" if current is None else
                      ("replaced" if current < revision else "unchanged_same_revision"),
            "previous_revision": current,
            "published_revision": revision,
            "withdrawal_superseded": fence,
            "rows": len(rows),
        }
    finally:
        conn.close()


def withdraw_published(
    db_path: str,
    section: str,
    date: str,
    expected_revision: int,
    reason: str,
    worker: str,
) -> dict:
    """Withdraw the currently published version of a day.

    Optimistic concurrency: ``expected_revision`` must equal the revision the
    reviewer saw. If a newer revision was published in the meantime the request
    is rejected (``RevisionConflictError``) and the new archive is untouched.
    The withdrawal audit row is inserted and the day's observations are deleted
    in one committed transaction; a failure leaves everything as it was.
    """
    conn = get_conn(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            active = conn.execute(
                "SELECT id, revision FROM withdrawals "
                "WHERE section=? AND date=? AND superseded_at IS NULL LIMIT 1",
                (section, date),
            ).fetchone()
            if active is not None:
                raise AlreadyWithdrawnError(
                    f"revision {int(active['revision'])} of {section} {date} is "
                    f"already withdrawn"
                )

            row = conn.execute(
                "SELECT revision FROM published WHERE section=? AND date=? LIMIT 1",
                (section, date),
            ).fetchone()
            if row is None:
                raise NoPublishedError(
                    f"no published version exists for {section} {date}"
                )
            current_revision = int(row["revision"])
            if current_revision != int(expected_revision):
                raise RevisionConflictError(int(expected_revision), current_revision)

            now = utc_now()
            cur = conn.execute(
                "INSERT INTO withdrawals (section, date, revision, reason, "
                "withdrawn_at, withdrawn_by) VALUES (?, ?, ?, ?, ?, ?)",
                (section, date, current_revision, reason, now, worker),
            )
            withdrawal_id = int(cur.lastrowid)
            # The old observations must stop being served immediately.
            conn.execute(
                "DELETE FROM published WHERE section=? AND date=?", (section, date)
            )
            conn.execute("COMMIT")
        except Exception:
            # Rejected optimistic checks and storage failures roll back here;
            # the previously published archive is never partially deleted.
            rollback_safely(conn)
            raise
        return {
            "withdrawal_id": withdrawal_id,
            "section": section,
            "date": date,
            "status": "withdrawn",
            "withdrawn_revision": current_revision,
            "reason": reason,
            "withdrawn_at": now,
            "withdrawn_by": worker,
        }
    finally:
        conn.close()


def get_run(db_path: str, run_id: int) -> Optional[dict]:
    conn = get_conn(db_path)
    try:
        row = conn.execute("SELECT * FROM runs WHERE id=?", (run_id,)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def list_runs(db_path: str, section: str, date: str) -> list[dict]:
    conn = get_conn(db_path)
    try:
        return [
            dict(r)
            for r in conn.execute(
                "SELECT * FROM runs WHERE section=? AND date=? ORDER BY id DESC",
                (section, date),
            ).fetchall()
        ]
    finally:
        conn.close()


def list_withdrawals(db_path: str, section: str, date: str) -> list[dict]:
    """Full withdrawal audit trail for a day (newest first)."""
    conn = get_conn(db_path)
    try:
        return [
            dict(r)
            for r in conn.execute(
                "SELECT * FROM withdrawals WHERE section=? AND date=? ORDER BY id DESC",
                (section, date),
            ).fetchall()
        ]
    finally:
        conn.close()


def get_published(db_path: str, section: str, date: str) -> Optional[dict]:
    """Return the day's current readable state from one committed snapshot.

    A single deferred read transaction (WAL snapshot) covers both the
    withdrawal record and the observation rows, so the two can never come from
    different committed states. Returns ``None`` only when the day has never
    been published and has never been withdrawn.
    """
    conn = get_conn(db_path)
    try:
        conn.execute("BEGIN")
        try:
            wrow = conn.execute(
                "SELECT * FROM withdrawals WHERE section=? AND date=? "
                "ORDER BY id DESC LIMIT 1",
                (section, date),
            ).fetchone()
            rows = conn.execute(
                "SELECT time, nh3n, source_page, revision, published_at, published_by "
                "FROM published WHERE section=? AND date=? ORDER BY time",
                (section, date),
            ).fetchall()
            conn.execute("COMMIT")
        except sqlite3.Error:
            rollback_safely(conn)
            raise
    finally:
        conn.close()

    latest_withdrawal = dict(wrow) if wrow is not None else None
    active_withdrawal = (
        latest_withdrawal
        if latest_withdrawal is not None and latest_withdrawal["superseded_at"] is None
        else None
    )

    if active_withdrawal is not None:
        # Withdrawn: downstream must not see any old observations, but the
        # reason and the withdrawn revision remain visible.
        return {
            "status": "withdrawn",
            "section": section,
            "date": date,
            "revision": None,
            "rows": [],
            "withdrawn_revision": active_withdrawal["revision"],
            "reason": active_withdrawal["reason"],
            "withdrawn_at": active_withdrawal["withdrawn_at"],
            "withdrawn_by": active_withdrawal["withdrawn_by"],
            "withdrawal_id": active_withdrawal["id"],
            "last_withdrawal": latest_withdrawal,
        }

    if not rows:
        return None

    first = rows[0]
    return {
        "status": "published",
        "section": section,
        "date": date,
        "revision": first["revision"],
        "published_at": first["published_at"],
        "published_by": first["published_by"],
        "rows": [
            {
                "time": r["time"],
                "nh3n": r["nh3n"],
                "source_page": r["source_page"],
                "revision": r["revision"],
            }
            for r in rows
        ],
        # Present when a previous withdrawal was superseded by this version.
        "last_withdrawal": latest_withdrawal,
    }
