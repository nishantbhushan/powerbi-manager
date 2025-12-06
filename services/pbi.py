import json
import os
import subprocess
import time

ROOT_DIR = os.path.dirname(os.path.dirname(__file__))
TENANT_ID = os.environ.get("PBI_TENANT_ID", "common")
POWERSHELL_SCRIPT = os.environ.get(
    "PBI_WORKSPACES_SCRIPT",
    os.path.join(ROOT_DIR, "backend", "Get-PBIWorkspaces.ps1"),
)
CACHE_SECONDS = int(os.environ.get("PBI_WORKSPACE_CACHE_SECONDS", "300"))
LOG_PATH = os.environ.get("PBI_LOG_PATH", os.path.join(ROOT_DIR, "ps_debug.log"))

workspace_cache = {"data": None, "expires": 0.0}


def _log(label, content):
    if not label:
        return
    try:
        with open(LOG_PATH, "a", encoding="utf-8") as fh:
            fh.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {label}\n")
            fh.write(content or "")
            if not (content or "").endswith("\n"):
                fh.write("\n")
    except Exception:
        pass


def _run_ps(args, label=None):
    cmd = [
        "powershell",
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        POWERSHELL_SCRIPT,
        "-TenantId",
        TENANT_ID,
    ] + args
    proc = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace")
    stdout = proc.stdout or ""
    stderr = proc.stderr or ""
    _log(label or "ps-call", stdout if stdout else stderr)
    if proc.returncode != 0:
        raise RuntimeError(f"PowerShell exited {proc.returncode}: {stderr or stdout}")
    return stdout


def fetch_workspaces():
    now = time.time()
    if workspace_cache["data"] is not None and now < workspace_cache["expires"]:
        return workspace_cache["data"]

    stdout = _run_ps([], label="workspaces")
    try:
        data = json.loads(stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Could not parse PowerShell output: {exc}\nRaw output:\n{stdout}")

    if "workspaces" not in data:
        raise RuntimeError(f"Unexpected response: {data}")

    workspace_cache["data"] = data["workspaces"]
    workspace_cache["expires"] = time.time() + CACHE_SECONDS
    return workspace_cache["data"]


def fetch_semantic_models(workspace_id: str):
    stdout = _run_ps(["-Mode", "models", "-WorkspaceId", workspace_id], label=f"models {workspace_id}")
    try:
        data = json.loads(stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Could not parse PowerShell output: {exc}\nRaw output:\n{stdout}")

    if "datasets" not in data:
        raise RuntimeError(f"Unexpected response: {data}")
    return data["datasets"]


def fetch_refreshes(workspace_id: str, dataset_id: str, top: int = 10):
    stdout = _run_ps(
        [
            "-Mode",
            "refreshes",
            "-WorkspaceId",
            workspace_id,
            "-DatasetId",
            dataset_id,
            "-Top",
            str(top),
        ],
        label=f"refreshes {workspace_id}/{dataset_id} top={top}",
    )
    try:
        data = json.loads(stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Could not parse PowerShell output: {exc}\nRaw output:\n{stdout}")

    if "refreshes" not in data:
        raise RuntimeError(f"Unexpected response: {data}")
    return data["refreshes"]


def trigger_refresh(workspace_id: str, dataset_id: str):
    stdout = _run_ps(
        [
            "-Mode",
            "trigger",
            "-WorkspaceId",
            workspace_id,
            "-DatasetId",
            dataset_id,
        ],
        label=f"trigger {workspace_id}/{dataset_id}",
    )
    try:
        data = json.loads(stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Could not parse PowerShell output: {exc}\nRaw output:\n{stdout}")

    return data


def fetch_workspace_reports(workspace_id: str):
    stdout = _run_ps(
        [
            "-Mode",
            "reports",
            "-WorkspaceId",
            workspace_id,
        ],
        label=f"reports {workspace_id}",
    )
    try:
        data = json.loads(stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Could not parse PowerShell output: {exc}\nRaw output:\n{stdout}")

    if "reports" not in data:
        raise RuntimeError(f"Unexpected response: {data}")
    return data["reports"]


def fetch_refresh_schedule(workspace_id: str, dataset_id: str):
    stdout = _run_ps(
        [
            "-Mode",
            "schedule",
            "-WorkspaceId",
            workspace_id,
            "-DatasetId",
            dataset_id,
        ],
        label=f"schedule-get {workspace_id}/{dataset_id}",
    )
    try:
        data = json.loads(stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Could not parse PowerShell output: {exc}\nRaw output:\n{stdout}")

    if "schedule" not in data:
        raise RuntimeError(f"Unexpected response: {data}")
    return data["schedule"]


def update_refresh_schedule(workspace_id: str, dataset_id: str, schedule_payload: dict):
    stdout = _run_ps(
        [
            "-Mode",
            "schedule",
            "-WorkspaceId",
            workspace_id,
            "-DatasetId",
            dataset_id,
            "-ScheduleJson",
            json.dumps(schedule_payload),
        ],
        label=f"schedule-set {workspace_id}/{dataset_id}",
    )
    try:
        data = json.loads(stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Could not parse PowerShell output: {exc}\nRaw output:\n{stdout}")
    return data


def takeover_dataset(workspace_id: str, dataset_id: str):
    stdout = _run_ps(
        [
            "-Mode",
            "takeover",
            "-WorkspaceId",
            workspace_id,
            "-DatasetId",
            dataset_id,
        ],
        label=f"takeover {workspace_id}/{dataset_id}",
    )
    try:
        data = json.loads(stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Could not parse PowerShell output: {exc}\nRaw output:\n{stdout}")
    return data
