# WiziAgent QA Platform — Context Document
*Last updated: March 2026*

---

## Live URLs
| | |
|---|---|
| Frontend (Vercel) | https://intentwise-qa.vercel.app |
| Backend (Railway) | https://intentwise-backend-production.up.railway.app |
| GitHub frontend | https://github.com/zubairsayed13/intentwise-qa |
| GitHub backend | https://github.com/zubairsayed13/intentwise-backend |

---

## Output Files
| File | Lines | Purpose |
|---|---|---|
| `wizi-v2.jsx` | ~7932 | Full React app — deploy as `App.js` |
| `main.py` | ~5060 | FastAPI backend |

---

## Stack

**Frontend**
- React (JSX, inline styles only — no CSS frameworks)
- Recharts via CDN (`window.Recharts`)
- XLSX via CDN script tag
- lucide-react (npm)
- `package.json` needs only: `react`, `react-dom`, `lucide-react`

**Backend**
- FastAPI + uvicorn + psycopg2 + httpx + asyncio
- OpenAI GPT-4o via `/api/ai/chat`
- LangGraph + langchain-openai (optional, gracefully degraded)
- Redshift (mws + public schemas)
- pytz for IST timezone handling

**Infrastructure**
- Frontend: Vercel (auto-deploy from GitHub main)
- Backend: Railway (always-on paid tier — scheduler requires persistent process)
- DB: AWS Redshift staging

---

## Architecture

### Frontend Core
```
WiziAgentApp          — root, owns activeTab, themeKey, globalStatus
  ThemeCtx            — provides T (themed) to sidebar, TC (forced white) to main
  SchemaCtx           — fetches /api/schema once on mount
  useLocal(key, def)  — localStorage persistence
  useSession(key,def) — sessionStorage persistence
```

**CRITICAL scroll rule:**
- `<main>` uses `overflow:"hidden"` — all tabs must manage their own scroll
- Tabs using `height:calc + overflow:hidden`: TriageTab, ApprovalsActivityTab, QueryTab
- Tabs using `overflowY:auto` on root: DashboardTab, MorningBriefTab, WorkflowsTab, ConfigureTab, EvalsTab, AdsSopTab

### Backend Core
- Built-in background scheduler (`_background_scheduler`) fires every 60s
- Fires `custom_workflow_cron_check()` and `sop_auto_trigger()` each cycle
- Connection registry: `get_connection(db_key="default")`
  - Env vars: `DB_{KEY}_HOST`, `DB_{KEY}_PORT`, `DB_{KEY}_DB`, `DB_{KEY}_USER`, `DB_{KEY}_PASSWORD`

---

## Nav Tabs

| ID | Label | Key | Notes |
|---|---|---|---|
| `brief` | Daily Brief | 1 | Auto-fetches, health sparkline, anomaly detection |
| `triage` | Triage & Monitor | 3 | 4 sub-views: Issues / Data Preview / Log / Monitor |
| `workflows` | Workflows | 4 | Built-ins + custom SQL workflows |
| `approvals` | Approvals & Activity | A | Merged: ApprovalQueue + Activity sub-tabs |
| `config` | Configure | 7 | Slack, threshold, DB connections + Evals inline |
| `query` | Data Explorer | 8 | Schema explorer + SQL editor + Uploads panel |

---

## Components

### Shared UI
`Badge`, `Card`, `Btn`, `StatusDot`, `Spinner`, `EmptyState`, `HelpTip`, `CommandPalette`, `Sidebar`, `HealthSparkline`

### Tab Components
| Component | Tab |
|---|---|
| `MorningBriefTab` | Daily Brief |
| `TriageTab` | Triage (Issues + Data Preview + Log) |
| `MonitorTab` | Triage sub-view 4 |
| `ApprovalsActivityTab` | Approvals wrapper |
| `ApprovalQueueTab` | Approvals sub-tab |
| `ActivityTab` | Activity sub-tab (alerts + fix log) |
| `WorkflowsTab` | Workflows list/detail/builder |
| `WorkflowDetail` | 2-panel run history view |
| `RunDetail` | Per-check results with sample rows |
| `AllRunsView` | Cross-workflow run log |
| `WorkflowBuilder` | Create/edit workflow with SQL checks |
| `AdsSopTab` | Full SOP with 7 phases, 5 gates |
| `ConfigureTab` | Settings |
| `EvalsInline` | Evals embedded in Configure |
| `QueryTab` | SQL editor + schema browser |
| `UploadPanel` | File upload (xlsx/csv/json → Redshift) |
| `CheckSetBuilder` | Monitor check set editor |
| `DashboardTab` | Widget-based dashboard (hidden from nav) |

---

## Workflow System

### Built-in Workflows (BUILTIN_WFS + BUILTIN_SEEDS)
Two built-ins defined in `BUILTIN_WFS` (display) and `BUILTIN_SEEDS` (saved to backend on mount):

**Daily Data Brief** (`id: "daily-brief"`)
- Checks: Failed downloads, stuck pending, not replicated, data freshness, null report types
- Schedule: `16:30 IST`
- Endpoint: `/api/workflow/daily-run`
- Editable via ✏ Edit — merges backend version (real checks) with display entry

**Ads Download SOP** (`id: "ads-sop"`)
- Always uses `/api/workflow/ads-sop` (gate workflow), never the SQL check runner
- 7 phases: detection → gate1 → pause_mage → gate2 → validation → gate3 → refresh → gate4 → resume_copy → gate5 → finalize
- 5 gates with `threading.Event` + configurable timeout (default 30min)
- Mage packages: Bluewheel ×3, Maryruth ×2 (dummy API, swap URLs later)
- AWS refresh: 5 named jobs (dummy, swap with IAM URLs later)
- GDS copies: 6 BigQuery jobs via Mage (dummy pipeline IDs)

### Custom Workflows (v2)
- Saved via `POST /api/custom-workflows/save/v2`
- Flat `checks: [{id, name, sql, pass_condition, severity}]` list
- Pass conditions: `"rows = 0"`, `"rows > 0"`, `"rows > N"`, `"value = 0"`, `"value > N"`
- Run via `POST /api/custom-workflows/{id}/run/v2`
- History via `GET /api/custom-workflows/history/v2?limit=100`

### Scheduling
`_cron_is_due()` supports:
- `"every N min"` / `"every N hour"` — interval-based
- `"HH:MM IST"` — daily at IST time
- `"0 11 * * *"` — 5-part cron (UTC)

Cron check uses v2 runner (`_run_workflow_checks`) if `wf.checks` exists, else legacy runner.

### AI in Workflows
- **AI Check Generator**: describe check in plain English → AI writes SQL + pass condition + severity
- **AI Suggest**: generates 3 workflow suggestions based on available tables
- **Test SQL**: runs query live against Redshift before adding check

---

## Persistence (Redshift)

All state persists to `wz_uploads` schema. Loaded back on startup via `_master_startup()`.

| Data | Table | Saved when |
|---|---|---|
| Custom workflows | `_wf_registry` | On every save |
| Workflow run history | `_wf_run_log` | After every run (background thread) |
| Dashboard widgets | `_dashboard_widgets` | On save/reorder |
| Anomaly baselines | `_schema` | After baseline build |
| Eval run history | `_eval_history` | After every eval run |
| SOP run state | `_sop_runs` | After every gate + step |

### In-memory stores (backed by Redshift)
```python
_CUSTOM_WORKFLOWS: dict   # loaded from _wf_registry on startup
_wf_run_history: list     # loaded from _wf_run_log on startup
_baselines: dict          # loaded from _schema on startup
_eval_history: list       # loaded from _eval_history on startup
_sop_runs: dict           # loaded from _sop_runs on startup
_sop_gate_store: dict     # NOT persisted (threading.Event — can't serialize)
```

---

## API Endpoints (Key)

### Workflows
```
POST /api/custom-workflows/save/v2       save/update workflow + persist to Redshift
GET  /api/custom-workflows               list all (in-memory)
GET  /api/custom-workflows/load-from-db  force reload from Redshift into memory
DELETE /api/custom-workflows/{id}        delete
POST /api/custom-workflows/{id}/run/v2   run workflow (uses flat checks)
GET  /api/custom-workflows/history/v2    run history (memory + Redshift merge)
POST /api/custom-workflows/cron-check/v2 manual cron trigger
POST /api/workflow/daily-run             run Daily Data Brief checks
POST /api/workflow/ads-sop               start SOP (non-blocking, returns run_id)
GET  /api/workflow/ads-sop/{run_id}      poll SOP state
POST /api/workflow/sop-gate              approve/reject/force a gate
POST /api/workflow/sop-gate-force        force all pending gates (testing)
```

### Data
```
GET  /api/schema            full Redshift schema string
GET  /api/tables            table list with metadata
GET  /api/query             execute SQL query
GET  /api/accounts          distinct account_id + seller_id
GET  /api/preview           table preview (50 rows)
POST /api/monitor/run-checks run check sets on a table
```

### AI
```
POST /api/ai/chat           proxy to GPT-4o
POST /api/fix-history/summary AI analysis of fix log
POST /api/wizi-agent/run-table LangGraph agent on single table
POST /api/wizi-agent/approve  approve high-risk agent fix
```

### Anomaly
```
POST /api/anomaly/baseline   build baselines (persisted to Redshift)
GET  /api/anomaly/check      compare current vs baseline
GET  /api/anomaly/baselines  return all baselines
```

### Uploads + Config
```
POST /api/uploads            upload file → Redshift wz_uploads.{slug}_{ts}
GET  /api/uploads            list uploaded tables
GET  /api/config/db-connections  list registered DB connections
POST /api/config/db-connections  register new DB connection
```

---

## Schema Groups
`amazon_source_data`, `instacart_source_data`, `cruteo_source_data`, `walmart_source_data`, `intentwise_ecommerce_graph`, `tiktok_source_data`, `meta_source_data`, `google_source_data`, `mws`, `public`, `wz_uploads`

---

## Environment Variables (Railway)
```
OPENAI_API_KEY
SLACK_WEBHOOK_URL
SOP_TRIGGER_TIME       (default "16:00" IST)
SOP_GATE_TIMEOUT_MIN   (default 30)
DB_DEFAULT_HOST / PORT / DB / USER / PASSWORD
DB_{KEY}_*             (for additional connections)
LANGCHAIN_API_KEY      (optional, LangSmith tracing)
LANGCHAIN_PROJECT      (optional)
```

---

## Build Rules (Critical)
1. **Never rebuild from scratch** — always integrate into existing file
2. **Always syntax check** before presenting: `acorn` (JSX) + `python3 ast` (PY)
3. **`<main>` is `overflow:hidden`** — every tab root needs its own scroll strategy
4. **recharts + xlsx via CDN** — no npm import
5. **Minimize scanning** — only read/change what's needed
6. **Flag before auto-fixing** — never auto-fix daily workflow without approval

---

## Pending / Known Gaps
- Mage API: dummy endpoints, needs real `MAGE_API_URL` + package IDs
- AWS refresh jobs: dummy, needs IAM-authenticated URLs
- GDS BigQuery copies: dummy Mage pipeline IDs
- Ask WiziAgent tab: dropped, to be rebuilt later
- Dashboard tab: built but hidden from nav
