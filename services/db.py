import os
import sqlite3
import json
from datetime import datetime

ROOT_DIR = os.path.dirname(os.path.dirname(__file__))
DB_PATH = os.environ.get("PBI_DB_PATH", os.path.join(ROOT_DIR, "categories.db"))


def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS categories (
            workspace_id TEXT PRIMARY KEY,
            env TEXT NOT NULL,
            module TEXT,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS semantic_models (
            workspace_id TEXT NOT NULL,
            model_id TEXT NOT NULL,
            name TEXT NOT NULL,
            added_at TEXT NOT NULL,
            deleted_at TEXT,
            PRIMARY KEY (workspace_id, model_id)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS refresh_history (
            workspace_id TEXT NOT NULL,
            dataset_id TEXT NOT NULL,
            start_time TEXT NOT NULL,
            end_time TEXT,
            status TEXT,
            duration_seconds REAL,
            recorded_at TEXT NOT NULL,
            PRIMARY KEY (workspace_id, dataset_id, start_time)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS reports (
            workspace_id TEXT NOT NULL,
            report_id TEXT NOT NULL,
            name TEXT,
            dataset_id TEXT,
            web_url TEXT,
            embed_url TEXT,
            created_at TEXT,
            PRIMARY KEY (workspace_id, report_id)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS schedules (
            workspace_id TEXT NOT NULL,
            dataset_id TEXT NOT NULL,
            schedule_json TEXT,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (workspace_id, dataset_id)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS capacity_metrics (
            capacity_id TEXT NOT NULL,
            ts TEXT NOT NULL,
            metric TEXT DEFAULT 'cu',
            value REAL,
            recorded_at TEXT NOT NULL,
            PRIMARY KEY (capacity_id, ts, metric)
        )
        """
    )
    conn.commit()
    conn.close()


def load_categories():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT workspace_id, env, module FROM categories")
    categories = {}
    for workspace_id, env, module in cur.fetchall():
        categories[workspace_id] = {"env": env, "module": module}
    conn.close()
    return categories


def upsert_category(workspace_id: str, env: str, module: str):
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """
        INSERT INTO categories(workspace_id, env, module, updated_at)
        VALUES (?, ?, ?, datetime('now'))
        ON CONFLICT(workspace_id) DO UPDATE SET
            env=excluded.env,
            module=excluded.module,
            updated_at=datetime('now')
        """,
        (workspace_id, env, module),
    )
    conn.commit()
    conn.close()


def load_semantic_models_by_workspace():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT workspace_id, model_id, name, added_at, deleted_at FROM semantic_models")
    data = {}
    for workspace_id, model_id, name, added_at, deleted_at in cur.fetchall():
        data.setdefault(workspace_id, []).append(
            {
                "model_id": model_id,
                "name": name,
                "added_at": added_at,
                "deleted_at": deleted_at,
            }
        )
    conn.close()
    return data


def update_semantic_models(workspace_id, models):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "SELECT model_id, name, added_at, deleted_at FROM semantic_models WHERE workspace_id = ?",
        (workspace_id,),
    )
    existing = {row[0]: {"name": row[1], "added_at": row[2], "deleted_at": row[3]} for row in cur.fetchall()}
    now = datetime.utcnow().isoformat()

    incoming_ids = set()
    for model in models:
        mid = model.get("id") or model.get("model_id")
        if not mid:
            continue
        incoming_ids.add(mid)
        current = existing.get(mid)
        added_at = current["added_at"] if current else now
        conn.execute(
            """
            INSERT INTO semantic_models(workspace_id, model_id, name, added_at, deleted_at)
            VALUES (?, ?, ?, ?, NULL)
            ON CONFLICT(workspace_id, model_id) DO UPDATE SET
                name=excluded.name,
                deleted_at=NULL
            """,
            (workspace_id, mid, model.get("name") or model.get("displayName") or "(unnamed)", added_at),
        )

    missing_ids = set(existing.keys()) - incoming_ids
    for mid in missing_ids:
        if existing[mid]["deleted_at"] is None:
            conn.execute(
                "UPDATE semantic_models SET deleted_at = ? WHERE workspace_id = ? AND model_id = ?",
                (now, workspace_id, mid),
            )

    conn.commit()
    conn.close()


def save_refreshes(workspace_id: str, dataset_id: str, refreshes: list):
    conn = sqlite3.connect(DB_PATH)
    for r in refreshes:
        start_time = r.get("startTime")
        end_time = r.get("endTime")
        status = r.get("status")
        duration_seconds = None
        if start_time and end_time:
            try:
                start_dt = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
                end_dt = datetime.fromisoformat(end_time.replace("Z", "+00:00"))
                duration_seconds = (end_dt - start_dt).total_seconds()
            except Exception:
                duration_seconds = None
        conn.execute(
            """
            INSERT OR REPLACE INTO refresh_history(workspace_id, dataset_id, start_time, end_time, status, duration_seconds, recorded_at)
            VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
            """,
            (workspace_id, dataset_id, start_time, end_time, status, duration_seconds),
        )
    conn.commit()
    conn.close()


def load_refreshes(workspace_id: str):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "SELECT dataset_id, start_time, end_time, status, duration_seconds FROM refresh_history WHERE workspace_id = ? ORDER BY start_time DESC",
        (workspace_id,),
    )
    data = {}
    for dataset_id, start_time, end_time, status, duration_seconds in cur.fetchall():
        data.setdefault(dataset_id, []).append(
            {
                "start_time": start_time,
                "end_time": end_time,
                "status": status,
                "duration_seconds": duration_seconds,
            }
        )
    conn.close()
    return data


def save_capacity_metrics(capacity_id: str, points: list):
    if not capacity_id or not points:
        return
    conn = sqlite3.connect(DB_PATH)
    for p in points:
        ts = p.get("ts") or p.get("timestamp")
        val = p.get("cu") if p.get("cu") is not None else p.get("value")
        metric = p.get("metric") or "cu"
        if not ts:
            continue
        conn.execute(
            """
            INSERT OR REPLACE INTO capacity_metrics(capacity_id, ts, metric, value, recorded_at)
            VALUES (?, ?, ?, ?, datetime('now'))
            """,
            (capacity_id, ts, metric, val),
        )
    conn.commit()
    conn.close()


def load_capacity_metrics(capacity_id: str, start_iso: str | None = None, end_iso: str | None = None):
    if not capacity_id:
        return []
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    sql = "SELECT ts, metric, value FROM capacity_metrics WHERE capacity_id = ?"
    args = [capacity_id]
    if start_iso:
        sql += " AND ts >= ?"
        args.append(start_iso)
    if end_iso:
        sql += " AND ts <= ?"
        args.append(end_iso)
    sql += " ORDER BY ts ASC"
    cur.execute(sql, args)
    data = []
    for ts, metric, value in cur.fetchall():
        data.append({"ts": ts, "metric": metric, "value": value})
    conn.close()
    return data


def save_reports(workspace_id: str, reports: list):
    if not workspace_id or reports is None:
        return
    conn = sqlite3.connect(DB_PATH)
    for rep in reports:
        conn.execute(
            """
            INSERT OR REPLACE INTO reports(workspace_id, report_id, name, dataset_id, web_url, embed_url, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                workspace_id,
                rep.get("id") or rep.get("report_id"),
                rep.get("name"),
                rep.get("datasetId") or rep.get("dataset_id"),
                rep.get("webUrl") or rep.get("web_url"),
                rep.get("embedUrl") or rep.get("embed_url"),
                rep.get("createdDate") or rep.get("created_at"),
            ),
        )
    conn.commit()
    conn.close()


def load_reports_by_workspace(workspace_id: str):
    if not workspace_id:
        return {}
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "SELECT report_id, name, dataset_id, web_url, embed_url, created_at FROM reports WHERE workspace_id = ?",
        (workspace_id,),
    )
    data = {}
    for report_id, name, dataset_id, web_url, embed_url, created_at in cur.fetchall():
        data.setdefault(dataset_id or "", []).append(
            {
                "id": report_id,
                "name": name,
                "datasetId": dataset_id,
                "webUrl": web_url,
                "embedUrl": embed_url,
                "created_at": created_at,
            }
        )
    conn.close()
    return data


def save_schedule(workspace_id: str, dataset_id: str, schedule: dict | str):
    if not workspace_id or not dataset_id:
        return
    sched_str = schedule if isinstance(schedule, str) else json.dumps(schedule)
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """
        INSERT OR REPLACE INTO schedules(workspace_id, dataset_id, schedule_json, updated_at)
        VALUES (?, ?, ?, datetime('now'))
        """,
        (workspace_id, dataset_id, sched_str),
    )
    conn.commit()
    conn.close()


def load_schedules_by_workspace(workspace_id: str):
    if not workspace_id:
        return {}
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "SELECT dataset_id, schedule_json FROM schedules WHERE workspace_id = ?",
        (workspace_id,),
    )
    data = {}
    for ds_id, sched in cur.fetchall():
        try:
            parsed = json.loads(sched)
        except Exception:
            parsed = sched
        data[ds_id] = parsed
    conn.close()
    return data
