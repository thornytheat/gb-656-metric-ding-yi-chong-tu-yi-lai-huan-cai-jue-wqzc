"""SQLite 读写：批次快照、运行清单、隔离行、键值状态、目录下载账。

只放数据访问，不放判定；调用方拿到的是普通字典。
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path

from app import config

_LOCK = threading.RLock()

SCHEMA = """
CREATE TABLE IF NOT EXISTS snapshots (
    schema_id      TEXT PRIMARY KEY,
    version_label  TEXT NOT NULL,
    source_path    TEXT NOT NULL,
    source_sha256  TEXT NOT NULL,
    encoding       TEXT NOT NULL,
    bom            INTEGER NOT NULL,
    field_count    INTEGER NOT NULL,
    blocked_count  INTEGER NOT NULL,
    created_at     TEXT NOT NULL,
    payload_json   TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS runs (
    run_id         TEXT PRIMARY KEY,
    batch_key      TEXT NOT NULL UNIQUE,
    old_schema_id  TEXT NOT NULL,
    new_schema_id  TEXT NOT NULL,
    version_seq    INTEGER NOT NULL,
    input_sha256   TEXT NOT NULL,
    ruleset        TEXT NOT NULL,
    created_at     TEXT NOT NULL,
    effective_from TEXT NOT NULL,
    effective_to   TEXT,
    summary_json   TEXT NOT NULL,
    payload_json   TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS quarantine (
    schema_id     TEXT NOT NULL,
    line          INTEGER,
    table_name    TEXT,
    column_name   TEXT,
    reason        TEXT NOT NULL,
    blocking      INTEGER NOT NULL,
    detail        TEXT,
    raw           TEXT
);
CREATE TABLE IF NOT EXISTS kv (
    k TEXT PRIMARY KEY,
    v TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS download_tasks (
    task_id        TEXT PRIMARY KEY,
    idem_key       TEXT NOT NULL UNIQUE,
    run_id         TEXT NOT NULL,
    status         TEXT NOT NULL,
    fragment_bytes INTEGER NOT NULL,
    total_bytes    INTEGER NOT NULL,
    generated_bytes INTEGER NOT NULL,
    fragment_count INTEGER NOT NULL,
    content_sha256 TEXT NOT NULL,
    created_at     TEXT NOT NULL,
    updated_at     TEXT NOT NULL,
    finished_at    TEXT,
    error          TEXT
);
CREATE TABLE IF NOT EXISTS download_fragments (
    task_id          TEXT NOT NULL,
    seq              INTEGER NOT NULL,
    byte_start       INTEGER NOT NULL,
    byte_end         INTEGER NOT NULL,
    sha256           TEXT NOT NULL,
    status           TEXT NOT NULL,
    content          BLOB,
    lease_token      TEXT,
    lease_expires_at TEXT,
    claimed_at       TEXT,
    reclaimed_at     TEXT,
    reclaim_reason   TEXT,
    PRIMARY KEY (task_id, seq)
);
CREATE INDEX IF NOT EXISTS idx_quarantine_schema ON quarantine(schema_id);
CREATE INDEX IF NOT EXISTS idx_dl_frag_task ON download_fragments(task_id);
CREATE INDEX IF NOT EXISTS idx_dl_frag_lease ON download_fragments(status, lease_expires_at);
"""


def db_path() -> Path:
    raw = os.environ.get("METRICGRAPH_DB", "").strip()
    path = Path(raw) if raw else config.DEFAULT_DB
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(db_path(), timeout=15)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db() -> None:
    with _LOCK, _connect() as conn:
        conn.executescript(SCHEMA)


@contextmanager
def locked():
    """复合操作的互斥区间：服务层在读-判-写期间持有，避免并发交错。"""
    with _LOCK:
        yield


def reset() -> None:
    """测试与重建用：清空全部业务表。"""
    with _LOCK, _connect() as conn:
        conn.executescript(SCHEMA)
        for table in ("snapshots", "runs", "quarantine", "kv",
                      "download_tasks", "download_fragments"):
            conn.execute(f"DELETE FROM {table}")


def _dumps(value) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _loads(value):
    if not value:
        return None
    return json.loads(value)


# ---------- 快照 ----------

def save_snapshot(snapshot: dict) -> None:
    with _LOCK, _connect() as conn:
        conn.execute(
            """INSERT INTO snapshots (schema_id, version_label, source_path, source_sha256,
                   encoding, bom, field_count, blocked_count, created_at, payload_json)
               VALUES (?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(schema_id) DO UPDATE SET
                   version_label=excluded.version_label,
                   source_path=excluded.source_path,
                   source_sha256=excluded.source_sha256,
                   encoding=excluded.encoding,
                   bom=excluded.bom,
                   field_count=excluded.field_count,
                   blocked_count=excluded.blocked_count,
                   created_at=excluded.created_at,
                   payload_json=excluded.payload_json""",
            (
                snapshot["schema_id"], snapshot["version_label"], snapshot["source_path"],
                snapshot["source_sha256"], snapshot["encoding"], int(bool(snapshot["bom"])),
                int(snapshot["field_count"]), int(snapshot["blocked_count"]),
                snapshot["created_at"], _dumps(snapshot),
            ),
        )


def load_snapshot(schema_id: str) -> dict | None:
    with _LOCK, _connect() as conn:
        row = conn.execute(
            "SELECT payload_json FROM snapshots WHERE schema_id=?", (schema_id,)
        ).fetchone()
    return _loads(row["payload_json"]) if row else None


def list_snapshots(limit: int = 20) -> list[dict]:
    with _LOCK, _connect() as conn:
        rows = conn.execute(
            """SELECT schema_id, version_label, source_path, source_sha256, encoding, bom,
                      field_count, blocked_count, created_at
               FROM snapshots ORDER BY created_at DESC, schema_id LIMIT ?""",
            (limit,),
        ).fetchall()
    return [dict(row) for row in rows]


def save_quarantine(schema_id: str, rows: list[dict]) -> None:
    with _LOCK, _connect() as conn:
        conn.execute("DELETE FROM quarantine WHERE schema_id=?", (schema_id,))
        conn.executemany(
            """INSERT INTO quarantine (schema_id, line, table_name, column_name, reason,
                   blocking, detail, raw) VALUES (?,?,?,?,?,?,?,?)""",
            [
                (
                    schema_id, int(row.get("line") or 0), row.get("table") or "",
                    row.get("column") or "", row["reason"], int(bool(row.get("blocking"))),
                    row.get("detail") or "", row.get("raw") or "",
                )
                for row in rows
            ],
        )


def list_quarantine(schema_id: str) -> list[dict]:
    with _LOCK, _connect() as conn:
        rows = conn.execute(
            """SELECT line, table_name, column_name, reason, blocking, detail, raw
               FROM quarantine WHERE schema_id=? ORDER BY line, reason""",
            (schema_id,),
        ).fetchall()
    return [
        {
            "line": row["line"],
            "table": row["table_name"],
            "column": row["column_name"],
            "reason": row["reason"],
            "blocking": bool(row["blocking"]),
            "detail": row["detail"],
            "raw": row["raw"],
        }
        for row in rows
    ]


# ---------- 运行清单 ----------

def save_run(run: dict) -> None:
    with _LOCK, _connect() as conn:
        conn.execute(
            """INSERT INTO runs (run_id, batch_key, old_schema_id, new_schema_id, version_seq,
                   input_sha256, ruleset, created_at, effective_from, effective_to,
                   summary_json, payload_json)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                run["run_id"], run["batch_key"], run["old_schema_id"], run["new_schema_id"],
                int(run["version_seq"]), run["input_sha256"], run["ruleset"],
                run["created_at"], run["effective_from"], run["effective_to"],
                _dumps(run["summary"]), _dumps(run),
            ),
        )


def find_run_by_key(batch_key: str) -> dict | None:
    with _LOCK, _connect() as conn:
        row = conn.execute(
            "SELECT payload_json FROM runs WHERE batch_key=?", (batch_key,)
        ).fetchone()
    return _loads(row["payload_json"]) if row else None


def run_by_id(run_id: str) -> dict | None:
    with _LOCK, _connect() as conn:
        row = conn.execute(
            "SELECT payload_json FROM runs WHERE run_id=?", (run_id,)
        ).fetchone()
    return _loads(row["payload_json"]) if row else None


def latest_run() -> dict | None:
    with _LOCK, _connect() as conn:
        row = conn.execute(
            "SELECT payload_json FROM runs ORDER BY version_seq DESC LIMIT 1"
        ).fetchone()
    return _loads(row["payload_json"]) if row else None


def list_runs(limit: int = 30) -> list[dict]:
    with _LOCK, _connect() as conn:
        rows = conn.execute(
            """SELECT summary_json FROM runs ORDER BY version_seq DESC, run_id LIMIT ?""",
            (limit,),
        ).fetchall()
    return [_loads(row["summary_json"]) for row in rows]


def count_runs() -> int:
    with _LOCK, _connect() as conn:
        row = conn.execute("SELECT COUNT(*) AS n FROM runs").fetchone()
    return int(row["n"])


def next_version_seq() -> int:
    with _LOCK, _connect() as conn:
        row = conn.execute("SELECT COALESCE(MAX(version_seq), 0) AS n FROM runs").fetchone()
    return int(row["n"]) + 1


def current_version_seq() -> int:
    with _LOCK, _connect() as conn:
        row = conn.execute("SELECT COALESCE(MAX(version_seq), 0) AS n FROM runs").fetchone()
    return int(row["n"])


def list_windows() -> list[dict]:
    with _LOCK, _connect() as conn:
        rows = conn.execute(
            "SELECT run_id, effective_from, effective_to FROM runs ORDER BY version_seq"
        ).fetchall()
    return [dict(row) for row in rows]


# ---------- 键值 ----------

def set_kv(key: str, value) -> None:
    with _LOCK, _connect() as conn:
        conn.execute(
            "INSERT INTO kv (k, v) VALUES (?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v",
            (key, _dumps(value)),
        )


def get_kv(key: str, default=None):
    with _LOCK, _connect() as conn:
        row = conn.execute("SELECT v FROM kv WHERE k=?", (key,)).fetchone()
    return _loads(row["v"]) if row else default


# ---------- 目录下载 ----------

_TASK_COLUMNS = (
    "task_id", "idem_key", "run_id", "status", "fragment_bytes", "total_bytes",
    "generated_bytes", "fragment_count", "content_sha256", "created_at",
    "updated_at", "finished_at", "error",
)

_FRAGMENT_COLUMNS = (
    "task_id", "seq", "byte_start", "byte_end", "sha256", "status", "content",
    "lease_token", "lease_expires_at", "claimed_at", "reclaimed_at",
    "reclaim_reason",
)


def _task_row(row: sqlite3.Row) -> dict:
    return {key: row[key] for key in _TASK_COLUMNS}


def _fragment_row(row: sqlite3.Row, with_content: bool = True) -> dict:
    out = {key: row[key] for key in _FRAGMENT_COLUMNS}
    if not with_content:
        out.pop("content")
    return out


def save_download_task(task: dict) -> None:
    with _LOCK, _connect() as conn:
        conn.execute(
            """INSERT INTO download_tasks (task_id, idem_key, run_id, status,
                   fragment_bytes, total_bytes, generated_bytes, fragment_count,
                   content_sha256, created_at, updated_at, finished_at, error)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(task_id) DO UPDATE SET
                   status=excluded.status,
                   generated_bytes=excluded.generated_bytes,
                   fragment_count=excluded.fragment_count,
                   updated_at=excluded.updated_at,
                   finished_at=excluded.finished_at,
                   error=excluded.error""",
            (
                task["task_id"], task["idem_key"], task["run_id"], task["status"],
                int(task["fragment_bytes"]), int(task["total_bytes"]),
                int(task["generated_bytes"]), int(task["fragment_count"]),
                task["content_sha256"], task["created_at"], task["updated_at"],
                task.get("finished_at"), task.get("error"),
            ),
        )


def download_task(task_id: str) -> dict | None:
    with _LOCK, _connect() as conn:
        row = conn.execute(
            f"SELECT {','.join(_TASK_COLUMNS)} FROM download_tasks WHERE task_id=?",
            (task_id,),
        ).fetchone()
    return _task_row(row) if row else None


def download_task_by_idem(idem_key: str) -> dict | None:
    with _LOCK, _connect() as conn:
        row = conn.execute(
            f"SELECT {','.join(_TASK_COLUMNS)} FROM download_tasks WHERE idem_key=?",
            (idem_key,),
        ).fetchone()
    return _task_row(row) if row else None


def list_download_tasks() -> list[dict]:
    with _LOCK, _connect() as conn:
        rows = conn.execute(
            f"SELECT {','.join(_TASK_COLUMNS)} FROM download_tasks"
            " ORDER BY created_at DESC, task_id"
        ).fetchall()
    return [_task_row(row) for row in rows]


def save_download_fragment(fragment: dict) -> None:
    with _LOCK, _connect() as conn:
        conn.execute(
            """INSERT INTO download_fragments (task_id, seq, byte_start, byte_end,
                   sha256, status, content, lease_token, lease_expires_at,
                   claimed_at, reclaimed_at, reclaim_reason)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(task_id, seq) DO UPDATE SET
                   status=excluded.status,
                   content=excluded.content,
                   lease_token=excluded.lease_token,
                   lease_expires_at=excluded.lease_expires_at,
                   claimed_at=excluded.claimed_at,
                   reclaimed_at=excluded.reclaimed_at,
                   reclaim_reason=excluded.reclaim_reason""",
            (
                fragment["task_id"], int(fragment["seq"]), int(fragment["byte_start"]),
                int(fragment["byte_end"]), fragment["sha256"], fragment["status"],
                fragment.get("content"), fragment.get("lease_token"),
                fragment.get("lease_expires_at"), fragment.get("claimed_at"),
                fragment.get("reclaimed_at"), fragment.get("reclaim_reason"),
            ),
        )


def save_download_fragment_and_task(fragment: dict, task: dict) -> None:
    """片段落盘与任务游标同事务，崩溃也不会出现片段已存而游标未走。"""
    with _LOCK, _connect() as conn:
        conn.execute(
            """INSERT INTO download_fragments (task_id, seq, byte_start, byte_end,
                   sha256, status, content, lease_token, lease_expires_at,
                   claimed_at, reclaimed_at, reclaim_reason)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(task_id, seq) DO UPDATE SET
                   status=excluded.status,
                   content=excluded.content,
                   lease_token=excluded.lease_token,
                   lease_expires_at=excluded.lease_expires_at,
                   claimed_at=excluded.claimed_at,
                   reclaimed_at=excluded.reclaimed_at,
                   reclaim_reason=excluded.reclaim_reason""",
            (
                fragment["task_id"], int(fragment["seq"]), int(fragment["byte_start"]),
                int(fragment["byte_end"]), fragment["sha256"], fragment["status"],
                fragment.get("content"), fragment.get("lease_token"),
                fragment.get("lease_expires_at"), fragment.get("claimed_at"),
                fragment.get("reclaimed_at"), fragment.get("reclaim_reason"),
            ),
        )
        conn.execute(
            """UPDATE download_tasks SET status=?, generated_bytes=?, fragment_count=?,
                   updated_at=?, finished_at=?, error=?
               WHERE task_id=?""",
            (
                task["status"], int(task["generated_bytes"]),
                int(task["fragment_count"]), task["updated_at"],
                task.get("finished_at"), task.get("error"), task["task_id"],
            ),
        )


def download_fragment(task_id: str, seq: int) -> dict | None:
    with _LOCK, _connect() as conn:
        row = conn.execute(
            f"SELECT {','.join(_FRAGMENT_COLUMNS)} FROM download_fragments"
            " WHERE task_id=? AND seq=?",
            (task_id, int(seq)),
        ).fetchone()
    return _fragment_row(row) if row else None


def list_download_fragments(task_id: str, with_content: bool = False) -> list[dict]:
    columns = tuple(key for key in _FRAGMENT_COLUMNS if with_content or key != "content")
    with _LOCK, _connect() as conn:
        rows = conn.execute(
            f"SELECT {','.join(columns)} FROM download_fragments"
            " WHERE task_id=? ORDER BY seq",
            (task_id,),
        ).fetchall()
    return [{key: row[key] for key in columns} for row in rows]


def list_expired_claims(now_iso: str) -> list[dict]:
    """到期仍挂着租约的片段，带上任务状态供回收判定。"""
    with _LOCK, _connect() as conn:
        rows = conn.execute(
            f"SELECT f.{',f.'.join(_FRAGMENT_COLUMNS)}, t.status AS task_status"
            " FROM download_fragments f JOIN download_tasks t ON t.task_id=f.task_id"
            " WHERE f.status='claimed' AND f.lease_expires_at<=?"
            " ORDER BY f.task_id, f.seq",
            (now_iso,),
        ).fetchall()
    out = []
    for row in rows:
        item = _fragment_row(row)
        item["task_status"] = row["task_status"]
        out.append(item)
    return out


def download_occupancy_rows() -> list[dict]:
    """占用账底表：活跃任务按预留计，收尾任务按仍被租约占着的片段计。"""
    with _LOCK, _connect() as conn:
        rows = conn.execute(
            """SELECT t.task_id, t.status, t.total_bytes,
                      COALESCE(SUM(CASE WHEN f.status='claimed'
                                        THEN f.byte_end - f.byte_start ELSE 0 END), 0)
                          AS claimed_bytes
               FROM download_tasks t
               LEFT JOIN download_fragments f ON f.task_id = t.task_id
               GROUP BY t.task_id, t.status, t.total_bytes
               ORDER BY t.task_id"""
        ).fetchall()
    return [dict(row) for row in rows]


def count_download_tasks() -> int:
    with _LOCK, _connect() as conn:
        row = conn.execute("SELECT COUNT(*) AS n FROM download_tasks").fetchone()
    return int(row["n"])
