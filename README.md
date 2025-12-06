# Power BI Automation (Flask + PowerShell)

This project exposes a small Flask UI and API to manage Power BI workspaces, semantic models, refresh history, schedules, performance analysis, and report dependencies. PowerShell handles all calls to the Power BI REST APIs (using `Connect-PowerBIServiceAccount` / `Get-PowerBIAccessToken`).

## Structure
- `app.py` – Flask app and routes (dashboard, categorize, workspace detail, dataset detail, performance analyzer, bulk fetch, refresh trigger, schedule set/get, capacity metrics ingest). Loads/saves data via `services/db.py` and calls PowerShell via `services/pbi.py`.
- `backend/Get-PBIWorkspaces.ps1` – PowerShell helper:
  - Auth with device code and 2-hour token cache.
  - Modes: `workspaces`, `models`, `refreshes`, `trigger` (dataOnly refresh), `reports`, `schedule` (GET/PATCH refreshSchedule), `takeover` (Default.TakeOver).
  - Returns JSON to stdout for the Python layer.
- `services/pbi.py` – Python wrapper to run the PS script; parses JSON; logs PS stdout/stderr to `ps_debug.log`. Functions: fetch workspaces/models/refreshes/reports/schedules, trigger refresh, takeover dataset, update schedule.
- `services/db.py` – SQLite helpers:
  - Tables: `categories`, `semantic_models`, `refresh_history`, `reports`, `schedules`, `capacity_metrics`.
  - Load/save categories, semantic models, refresh history, reports, schedules, capacity metrics.
- `templates/` – Bootstrap 5 UI:
  - `base.html` layout and nav.
  - `dashboard.html` categorized workspace tiles.
  - `categorize.html` bulk assign env/module.
  - `workspace.html` per-workspace models with last refresh, fetch history, trigger refresh, schedules, reports.
  - `dataset.html` per-model trend/table with refresh trigger.
  - `performance.html` performance analyzer (tabs 24h/7d/all, slow/fail charts, per-env 24h refresh charts, capacity line).
- `requirements.txt` – Python dependencies.
- `.gitignore` – excludes venv, db, logs, token cache.
- `ps_debug.log`, `categories.db`, `backend/pbi_token_cache.json` – local artifacts (ignored in git).

## Environment variables
- `PBI_TENANT_ID` (default `common`) – tenant for auth.
- `PBI_WORKSPACES_SCRIPT` – path to PS helper (default `backend/Get-PBIWorkspaces.ps1`).
- `PBI_WORKSPACE_CACHE_SECONDS` – cache for workspace list (default 300).
- `PBI_DB_PATH` – SQLite path (default `categories.db`).
- `PBI_CAPACITY_ID` – optional capacity id for capacity metrics.
- `PBI_LOG_PATH` – log path for PS stdout/stderr (default `ps_debug.log`).

## Running locally
```powershell
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
$env:PBI_TENANT_ID="common"              # adjust as needed
flask run                               # or python app.py
```
On first call, PowerShell prompts a device code login via `Connect-PowerBIServiceAccount`.

## Common PS invocations (direct)
```powershell
.\backend\Get-PBIWorkspaces.ps1                             # list workspaces
.\backend\Get-PBIWorkspaces.ps1 -Mode models -WorkspaceId "<ws>"
.\backend\Get-PBIWorkspaces.ps1 -Mode refreshes -WorkspaceId "<ws>" -DatasetId "<ds>" -Top 10
.\backend\Get-PBIWorkspaces.ps1 -Mode trigger   -WorkspaceId "<ws>" -DatasetId "<ds>"
.\backend\Get-PBIWorkspaces.ps1 -Mode reports   -WorkspaceId "<ws>"
# Get/update schedule
.\backend\Get-PBIWorkspaces.ps1 -Mode schedule -WorkspaceId "<ws>" -DatasetId "<ds>"
$body = @{ value = @{ days=@("Monday"); times=@("07:00","19:00"); localTimeZoneId="UTC" } } | ConvertTo-Json -Depth 5
.\backend\Get-PBIWorkspaces.ps1 -Mode schedule -WorkspaceId "<ws>" -DatasetId "<ds>" -ScheduleJson $body
```

## Key behaviors
- Refreshes are persisted; first fetch pulls 100 if none exist, else 10.
- Schedules are stored in SQLite for display; updates attempt dataset takeover before PATCHing.
- Performance analyzer tabs (24h/7d/all) compute slow/fail/outlier stats and show per-env refresh charts.
- Bulk fetch buttons exist on dashboard, workspace, and performance pages; progress messages appear in-page.

## Notes
- Requires permissions to read/update datasets; schedule updates may need ownership (auto-takeover attempted).
- Device code auth avoids storing client secrets.
