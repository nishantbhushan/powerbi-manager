import os 
import os
from datetime import datetime, timedelta, timezone
from flask import Flask, jsonify, render_template, request

from services.db import (
    init_db,
    load_categories,
    load_capacity_metrics,
    load_refreshes,
    load_semantic_models_by_workspace,
    load_schedules_by_workspace,
    load_reports_by_workspace,
    save_capacity_metrics,
    save_reports,
    save_refreshes,
    save_schedule,
    update_semantic_models,
    upsert_category,
)
from services.pbi import (
    CACHE_SECONDS,
    fetch_refreshes,
    fetch_refresh_schedule,
    takeover_dataset,
    fetch_semantic_models,
    fetch_workspace_reports,
    fetch_workspaces,
    update_refresh_schedule,
    trigger_refresh,
)

app = Flask(__name__)
CAPACITY_ID = os.environ.get("PBI_CAPACITY_ID")


def format_error(exc: Exception) -> str:
    return str(exc)


def build_summary(workspaces, categories, semantic_models, refreshes_by_ws):
    summary = {}
    ws_by_id = {ws.get("id"): ws for ws in workspaces}
    ws_stats = {}

    for ws_id, cat in categories.items():
        ws = ws_by_id.get(ws_id)
        if not ws:
            continue

        models = semantic_models.get(ws_id, [])
        refreshes = refreshes_by_ws.get(ws_id, {})

        failed_models = []
        slow_models = []

        for m in models:
            mid = m.get("model_id") or m.get("id")
            rlist = refreshes.get(mid) or []
            if rlist:
                latest = rlist[0]
                if (latest.get("status") or "").lower() != "completed":
                    failed_models.append(m.get("name") or mid)
                durations = [r.get("duration_seconds") for r in rlist if r.get("duration_seconds") is not None]
                if durations:
                    avg = sum(durations) / len(durations)
                    if (latest.get("duration_seconds") or 0) > avg * 1.1:
                        slow_models.append(m.get("name") or mid)

        stats = {
            "model_count": len(models),
            "failed_count": len(failed_models),
            "slow_count": len(slow_models),
            "failed_models": failed_models,
            "slow_models": slow_models,
        }
        ws_stats[ws_id] = stats

        module = cat.get("module") or "Unassigned module"
        env = cat.get("env") or "unspecified"
        summary.setdefault(module, {})
        summary[module].setdefault(env, [])
        summary[module][env].append(
            {
                "id": ws_id,
                "name": ws.get("name"),
                "models": models,
                **stats,
            }
        )
    return summary, ws_stats


def build_performance(workspaces, categories, semantic_models, refreshes_by_ws, skip_empty: bool = False):
    ws_by_id = {ws.get("id"): ws for ws in workspaces}
    models = []

    for ws_id, models_list in semantic_models.items():
        ws = ws_by_id.get(ws_id, {})
        cat = categories.get(ws_id, {})
        env = (cat.get("env") or "").upper()
        module = cat.get("module") or "Unassigned"
        refresh_map = refreshes_by_ws.get(ws_id, {})

        for m in models_list:
            mid = m.get("model_id") or m.get("id")
            rlist = refresh_map.get(mid) or []
            if skip_empty and not rlist:
                continue
            failures = len([r for r in rlist if (r.get("status") or "").lower() != "completed"])
            successes = len([r for r in rlist if (r.get("status") or "").lower() == "completed"])
            durations = [r.get("duration_seconds") for r in rlist if r.get("duration_seconds") is not None]
            avg_sec = sum(durations) / len(durations) if durations else 0
            last = rlist[0] if rlist else {}
            last_sec = last.get("duration_seconds") or 0
            outlier = avg_sec > 0 and last_sec > avg_sec * 1.1
            efficient = failures == 0 and avg_sec > 0 and avg_sec <= 300

            # frequency/interval based on refresh history
            freq_per_hour = 0
            avg_interval_hours = 0
            if rlist:
                try:
                    timestamps = []
                    for r in rlist:
                        ts = r.get("start_time") or r.get("startTime")
                        if not ts:
                            continue
                        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                        if dt.tzinfo is None:
                            dt = dt.replace(tzinfo=timezone.utc)
                        else:
                            dt = dt.astimezone(timezone.utc)
                        timestamps.append(dt)
                    timestamps.sort()
                    if len(timestamps) >= 2:
                        deltas = []
                        for i in range(1, len(timestamps)):
                            deltas.append((timestamps[i] - timestamps[i - 1]).total_seconds() / 3600.0)
                        avg_interval_hours = sum(deltas) / len(deltas) if deltas else 0
                        freq_per_hour = 0 if avg_interval_hours == 0 else 1 / avg_interval_hours
                except Exception:
                    freq_per_hour = 0
                    avg_interval_hours = 0

            models.append(
                {
                    "workspace_id": ws_id,
                    "workspace_name": ws.get("name") or ws_id,
                    "env": env,
                    "module": module,
                    "model_id": mid,
                    "model_name": m.get("name") or mid,
                    "avg_sec": avg_sec,
                    "last_sec": last_sec,
                    "failures": failures,
                    "successes": successes,
                    "total": len(rlist),
                    "freq_per_hour": freq_per_hour,
                    "avg_interval_hours": avg_interval_hours,
                    "last_status": last.get("status"),
                    "outlier": outlier,
                    "efficient": efficient,
                }
            )

    return models


def filter_refreshes_by_window(refreshes_by_ws, cutoff: datetime | None):
    if not cutoff:
        return refreshes_by_ws
    filtered = {}
    for ws_id, datasets in refreshes_by_ws.items():
        filtered[ws_id] = {}
        for ds_id, rlist in datasets.items():
            filtered_list = []
            for r in rlist:
                ts = r.get("start_time") or r.get("startTime")
                if not ts:
                    continue
                try:
                    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    else:
                        dt = dt.astimezone(timezone.utc)
                except Exception:
                    continue
                if dt >= cutoff:
                    filtered_list.append(r)
            filtered[ws_id][ds_id] = filtered_list
    return filtered


def build_capacity_series(capacity_metrics, cutoff: datetime | None):
    if not capacity_metrics:
        return []
    series = []
    for p in capacity_metrics:
        ts = p.get("ts") or p.get("timestamp")
        val = p.get("cu") if p.get("cu") is not None else p.get("value")
        if ts is None or val is None:
            continue
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            else:
                dt = dt.astimezone(timezone.utc)
        except Exception:
            continue
        if cutoff is None or dt >= cutoff:
            series.append({"x": ts, "y": val})
    series.sort(key=lambda p: p["x"])
    return series


def performance_sets(workspaces, categories, semantic_models, refreshes_by_ws, capacity_metrics=None):
    now = datetime.now(timezone.utc)
    ws_lookup = {w.get("id"): w.get("name") for w in workspaces}
    env_lookup = {ws_id: (categories.get(ws_id, {}).get("env") or "").upper() for ws_id in categories}
    windows = {
        "24h": now - timedelta(days=1),
        "7d": now - timedelta(days=7),
        "all": None,
    }
    result = {}
    for key, cutoff in windows.items():
        filtered = filter_refreshes_by_window(refreshes_by_ws, cutoff)
        models = build_performance(workspaces, categories, semantic_models, filtered, skip_empty=False)
        top_slow = sorted([m for m in models if m["avg_sec"] > 0], key=lambda x: x["avg_sec"], reverse=True)[:10]
        top_fail = sorted([m for m in models if m["failures"] > 0], key=lambda x: x["failures"], reverse=True)[:10]
        efficient = [m for m in models if m["efficient"]]
        outliers = [m for m in models if m["outlier"]]

        history24 = []
        window_cutoff = now - timedelta(hours=24)
        for ws_id, ds_map in filtered.items():
            ws_name = ws_lookup.get(ws_id, ws_id)
            env = env_lookup.get(ws_id, "")
            model_lookup = {m.get("model_id") or m.get("id"): m.get("name") for m in semantic_models.get(ws_id, [])}
            for ds_id, rlist in ds_map.items():
                if not rlist:
                    continue
                # include only refreshes in last 24h regardless of count
                points = []
                for r in rlist:
                    ts = r.get("start_time") or r.get("startTime")
                    if not ts:
                        continue
                    try:
                        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                        if dt.tzinfo is None:
                            dt = dt.replace(tzinfo=timezone.utc)
                        else:
                            dt = dt.astimezone(timezone.utc)
                    except Exception:
                        continue
                    if dt < window_cutoff:
                        continue
                    dur = (r.get("duration_seconds") or 0) / 60.0
                    points.append({"x": ts, "y": dur})
                if not points:
                    continue
                points.sort(key=lambda p: p["x"])
                history24.append(
                    {
                        "label": f"{model_lookup.get(ds_id, ds_id)} ({ws_name})",
                        "env": env,
                        "data": points,
                    }
                )
        cap_series = build_capacity_series(capacity_metrics or [], cutoff)

        result[key] = {
            "models": models,
            "top_slow": top_slow,
            "top_fail": top_fail,
            "efficient": efficient,
            "outliers": outliers,
            "history24": history24,
            "capacity": cap_series,
        }
    return result


def compute_avg_interval_hours(refreshes_by_ds):
    freq = {}
    for ds_id, rlist in refreshes_by_ds.items():
        timestamps = []
        for r in rlist:
            ts = r.get("start_time") or r.get("startTime")
            if not ts:
                continue
            try:
                dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                else:
                    dt = dt.astimezone(timezone.utc)
                timestamps.append(dt)
            except Exception:
                continue
        timestamps.sort()
        if len(timestamps) >= 2:
            deltas = []
            for i in range(1, len(timestamps)):
                deltas.append((timestamps[i] - timestamps[i - 1]).total_seconds() / 3600.0)
            freq[ds_id] = sum(deltas) / len(deltas) if deltas else 0
        else:
            freq[ds_id] = 0
    return freq


@app.route("/")
def dashboard():
    categories = load_categories()
    semantic_models = load_semantic_models_by_workspace()
    # preload refreshes for all categorized workspaces
    refreshes_by_ws = {ws_id: load_refreshes(ws_id) for ws_id in categories.keys()}
    try:
        workspaces = fetch_workspaces()
        error = None
    except Exception as exc:  # pylint: disable=broad-except
        workspaces = []
        error = format_error(exc)

    summary, ws_stats = build_summary(workspaces, categories, semantic_models, refreshes_by_ws)

    return render_template(
        "dashboard.html",
        workspaces=workspaces,
        categories=categories,
        summary=summary,
        semantic_models=semantic_models,
        ws_stats=ws_stats,
        error=error,
        CACHE_SECONDS=CACHE_SECONDS,
    )


@app.route("/performance")
def performance():
    categories = load_categories()
    semantic_models = load_semantic_models_by_workspace()
    ws_ids = set(categories.keys()) | set(semantic_models.keys())
    refreshes_by_ws = {ws_id: load_refreshes(ws_id) for ws_id in ws_ids}
    capacity_metrics = load_capacity_metrics(CAPACITY_ID)
    try:
        workspaces = fetch_workspaces()
        error = None
    except Exception as exc:  # pylint: disable=broad-except
        workspaces = []
        error = format_error(exc)

    perf = performance_sets(workspaces, categories, semantic_models, refreshes_by_ws, capacity_metrics)

    return render_template(
        "performance.html",
        perf=perf,
        error=error,
    )


@app.route("/categorize", methods=["GET"])
def categorize_page():
    categories = load_categories()
    try:
        workspaces = fetch_workspaces()
        error = None
    except Exception as exc:  # pylint: disable=broad-except
        workspaces = []
        error = format_error(exc)

    uncategorized = workspaces  # show all
    modules = sorted({v["module"].strip() for v in categories.values() if v.get("module")}, key=lambda x: x.lower())

    return render_template(
        "categorize.html",
        workspaces=uncategorized,
        categories=categories,
        modules=modules,
        error=error,
        CACHE_SECONDS=CACHE_SECONDS,
    )


@app.route("/api/workspaces")
def api_workspaces():
    categories = load_categories()
    try:
        workspaces = fetch_workspaces()
    except Exception as exc:  # pylint: disable=broad-except
        return jsonify({"ok": False, "message": format_error(exc)}), 500

    return jsonify({"ok": True, "workspaces": workspaces, "categories": categories})


@app.route("/categorize", methods=["POST"])
def categorize_api():
    payload = request.get_json(silent=True) or request.form
    workspace_id = payload.get("id") if payload else None
    env = (payload.get("env") or "").lower() if payload else ""
    module = payload.get("module") if payload else ""

    if not workspace_id or env not in {"dev", "uat", "prod"}:
        return (
            jsonify({"ok": False, "message": "id and env (dev/uat/prod) are required"}),
            400,
        )

    upsert_category(workspace_id, env, module)
    categories = load_categories()
    return jsonify({"ok": True, "category": categories.get(workspace_id), "categories": categories})


@app.route("/categorize/bulk", methods=["POST"])
def categorize_bulk():
    payload = request.get_json(silent=True) or {}
    items = payload.get("items") or []
    updated = []
    for item in items:
        wsid = item.get("id")
        env = (item.get("env") or "").lower()
        module = item.get("module") or ""
        if not wsid or env not in {"dev", "uat", "prod"}:
            continue
        upsert_category(wsid, env, module)
        updated.append(wsid)
    categories = load_categories()
    return jsonify({"ok": True, "updated": updated, "categories": categories})


@app.route("/fetch-models/<workspace_id>", methods=["POST"])
def fetch_models_api(workspace_id):
    try:
        models = fetch_semantic_models(workspace_id)
        update_semantic_models(workspace_id, models)
        semantic_models = load_semantic_models_by_workspace()
        return jsonify({"ok": True, "models": semantic_models.get(workspace_id, [])})
    except Exception as exc:  # pylint: disable=broad-except
        return jsonify({"ok": False, "message": format_error(exc)}), 500


@app.route("/fetch-refreshes/<workspace_id>/<dataset_id>", methods=["POST"])
def fetch_refreshes_api(workspace_id, dataset_id):
    try:
        current = load_refreshes(workspace_id).get(dataset_id, [])
        top = 10 if current else 100
        refreshes = fetch_refreshes(workspace_id, dataset_id, top=top)
        save_refreshes(workspace_id, dataset_id, refreshes)
        data = load_refreshes(workspace_id).get(dataset_id, [])
        return jsonify({"ok": True, "refreshes": data})
    except Exception as exc:  # pylint: disable=broad-except
        return jsonify({"ok": False, "message": format_error(exc)}), 500


@app.route("/schedule/<workspace_id>/<dataset_id>", methods=["GET"])
def get_schedule(workspace_id, dataset_id):
    try:
        schedule = fetch_refresh_schedule(workspace_id, dataset_id)
        return jsonify({"ok": True, "schedule": schedule})
    except Exception as exc:  # pylint: disable=broad-except
        return jsonify({"ok": False, "message": format_error(exc)}), 500


@app.route("/schedule/<workspace_id>/<dataset_id>", methods=["POST"])
def set_schedule(workspace_id, dataset_id):
    payload = request.get_json(silent=True) or {}
    if not payload:
        return jsonify({"ok": False, "message": "schedule payload required"}), 400
    try:
        try:
            takeover_dataset(workspace_id, dataset_id)
        except Exception:
            pass
        result = update_refresh_schedule(workspace_id, dataset_id, payload)
        try:
            save_schedule(workspace_id, dataset_id, payload)
        except Exception:
            pass
        return jsonify({"ok": True, "result": result})
    except Exception as exc:  # pylint: disable=broad-except
        return jsonify({"ok": False, "message": format_error(exc)}), 500


@app.route("/schedule-workspace/<workspace_id>", methods=["POST"])
def set_workspace_schedule(workspace_id):
    payload = request.get_json(silent=True) or {}
    if not payload:
        return jsonify({"ok": False, "message": "schedule payload required"}), 400
    semantic_models = load_semantic_models_by_workspace().get(workspace_id, [])
    updated = []
    failed = {}
    for m in semantic_models:
        mid = m.get("model_id") or m.get("id")
        if not mid:
            continue
        try:
            try:
                takeover_dataset(workspace_id, mid)
            except Exception:
                pass
            update_refresh_schedule(workspace_id, mid, payload)
            try:
                save_schedule(workspace_id, mid, payload)
            except Exception:
                pass
            updated.append(mid)
        except Exception as exc:  # pylint: disable=broad-except
            failed[mid] = format_error(exc)
            continue
    return jsonify({"ok": True, "updated": updated, "failed": failed})


@app.route("/capacity-metrics", methods=["POST"])
def capacity_metrics_ingest():
    payload = request.get_json(silent=True) or {}
    cap_id = payload.get("capacity_id") or CAPACITY_ID
    points = payload.get("points") if isinstance(payload, dict) else payload
    if isinstance(points, dict):
        points = [points]
    if not cap_id:
        return jsonify({"ok": False, "message": "capacity_id missing (set PBI_CAPACITY_ID or pass in body)"}), 400
    if not points:
        return jsonify({"ok": False, "message": "no points to save"}), 400
    save_capacity_metrics(cap_id, points)
    return jsonify({"ok": True, "saved": len(points)})


@app.route("/fetch-reports/<workspace_id>", methods=["POST"])
def fetch_reports_api(workspace_id):
    try:
        reports = fetch_workspace_reports(workspace_id)
        save_reports(workspace_id, reports)
        data = load_reports_by_workspace(workspace_id)
        return jsonify({"ok": True, "reports": data})
    except Exception as exc:  # pylint: disable=broad-except
        return jsonify({"ok": False, "message": format_error(exc)}), 500


@app.route("/workspace/<workspace_id>")
def workspace_detail(workspace_id):
    categories = load_categories()
    semantic_models = load_semantic_models_by_workspace()
    refreshes = load_refreshes(workspace_id)
    avg_interval_hours = compute_avg_interval_hours(refreshes)
    schedules = load_schedules_by_workspace(workspace_id)
    try:
        workspaces = fetch_workspaces()
        error = None
    except Exception as exc:  # pylint: disable=broad-except
        workspaces = []
        error = format_error(exc)

    ws = next((w for w in workspaces if w.get("id") == workspace_id), None)
    if not ws:
        return "Workspace not found", 404

    models = semantic_models.get(workspace_id, [])
    reports_by_model = load_reports_by_workspace(workspace_id)

    return render_template(
        "workspace.html",
        workspace=ws,
        models=models,
        refreshes=refreshes,
        reports_by_model=reports_by_model,
        schedules=schedules,
        avg_interval_hours=avg_interval_hours,
        categories=categories,
        error=error,
    )


@app.route("/workspace/<workspace_id>/dataset/<dataset_id>")
def dataset_detail(workspace_id, dataset_id):
    do_refresh = request.args.get("refresh", "1") != "0"
    try:
        workspaces = fetch_workspaces()
        workspace = next((w for w in workspaces if w.get("id") == workspace_id), None)
    except Exception as exc:  # pylint: disable=broad-except
        workspace = None
        error = format_error(exc)
    else:
        error = None

    semantic_models = load_semantic_models_by_workspace()
    model_list = semantic_models.get(workspace_id, [])
    dataset = next((m for m in model_list if m.get("model_id") == dataset_id or m.get("id") == dataset_id), None)

    if do_refresh and workspace:
        try:
            current = load_refreshes(workspace_id).get(dataset_id, [])
            top = 10 if current else 100
            data = fetch_refreshes(workspace_id, dataset_id, top=top)
            save_refreshes(workspace_id, dataset_id, data)
        except Exception as exc:  # pylint: disable=broad-except
            error = format_error(exc)

    refreshes = load_refreshes(workspace_id).get(dataset_id, [])

    return render_template(
        "dataset.html",
        workspace=workspace,
        dataset=dataset,
        dataset_id=dataset_id,
        refreshes=refreshes,
        error=error,
    )


@app.route("/refresh-model/<workspace_id>/<dataset_id>", methods=["POST"])
def refresh_model(workspace_id, dataset_id):
    try:
        trigger_refresh(workspace_id, dataset_id)
    except NotImplementedError:
        return jsonify({"ok": False, "message": "Trigger refresh not implemented in PowerShell helper"}), 501
    except Exception as exc:  # pylint: disable=broad-except
        return jsonify({"ok": False, "message": format_error(exc)}), 500
    return jsonify({"ok": True})


if __name__ == "__main__":
    init_db()
    port = int(os.environ.get("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=True)
else:
    init_db()
