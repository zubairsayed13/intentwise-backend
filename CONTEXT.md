# WiziAgent Dashboard — Session Context

## Project Overview
Full-stack data quality dashboard for Intentwise/Amazon Seller data on Redshift.
- **Frontend**: `App.jsx` (~17,800 lines) — React single-file app, no build step
- **Backend**: `main.py` (~8,050 lines) — FastAPI + psycopg2 + Redshift
- **Deploy**: Vercel (frontend) + Railway (backend)
- **GitHub**: zubairsayed13/intentwise-qa (frontend) + zubairsayed13/intentwise-backend
- **Working files on disk**: `/home/claude/wizi-v2-final.jsx`, `/home/claude/main-final.py`
- **Outputs**: `/mnt/user-data/outputs/App.jsx`, `/mnt/user-data/outputs/main.py`

---

## Architecture

### Frontend Tabs (NAV array)
`brief, triage, workflows, dataflows, config, query, results, scheduler, demo`

Shortcut keys: `1→brief, 2→triage, 3→workflows, 5→dataflows, 7→config, 8→query, 9→results, S→scheduler, D→demo`
> **BUG**: SHORTCUT_MAP is broken — maps wrong keys, missing results/scheduler/demo. Needs fix.

### Key Components
- `WiziAgentApp` — root, holds `activeTab`, `proactiveAlerts`, `showWizard`, `schemaStr`
- `Sidebar` — 220px (52 collapsed), dark gradient, 8 themes, `BellButton`
- `FloatingAssistant` — bottom-right chat, 11 agent tools, cross-tab session context
- `AiBriefPanel` — top of Daily Brief, health score + priority items
- `OnboardingWizard` — first-visit 3-question AI setup flow
- `NotifProvider` — toast stack + notification drawer (bell icon)
- `ConfirmModal` + `useConfirm()` — replaces window.confirm() (component added, not yet wired to all 11 confirm() calls)

### Contexts
- `ThemeCtx` → `useT()` — current theme object
- `SchemaCtx` → `useSchema()` — full Redshift schema string `"schema.table(col1,col2,...); ..."`

---

## Themes (8 total)
`light` (yellow accent), `solar`, `flame` (orange), `citrus` (green), `berry` (purple), `ocean` (cyan), `dark` (indigo, isDark:true), `midnight` (sky blue, isDark:true)

All themes have: `bg, surface, card, border, border2, text, text2, muted, dim, accent, accentL, green, red, orange, yellow, purple, cyan, sidebarBg, navActiveBg, navActiveText, navInactiveText, font, monoFont, googleFonts, wallpaper`

---

## Key Utility Components
```
Badge({ label, color, pulse })        — pill badge, solid fills for severity labels
Card({ children, style, onClick, hoverable }) — glassmorphism in dark, elevation in light
Btn({ children, onClick, variant, size, disabled, style }) — no hover useState
Spinner({ size, color })              — cubic-bezier easing
EmptyState({ icon, title, desc, action })
Skeleton({ w, h, r, style })          — shimmer loader
ConfirmModal / useConfirm()           — async confirmation dialog
```

### Hooks
```
useT()          — current theme
useSchema()     — dbSchema string
useLocal(key, def)    — localStorage state
useSession(key, def)  — sessionStorage state
useDebounce(value, delay=150) — debounce hook
```

---

## Performance Fixes Applied
- `Card` and `Btn` use CSS hover (no useState re-renders)
- `GLOBAL_CSS` injected once via useEffect into document.head
- Sidebar NAV items wrapped in `React.useMemo([active, pendingCount])`
- `<main>` has `contain: layout style`
- `useDebounce` applied to WorkflowsTab search

---

## AI Features Map (186 touchpoints)

### Per-tab AI
| Tab | Feature | Tokens | Trigger |
|-----|---------|--------|---------|
| Dashboard | KPI narrative card | 80 | On load |
| Daily Brief | Top 3 actions (zero tokens) | 0 | Always |
| Daily Brief | Proactive anomaly banner | 0 | Polling |
| Triage | Issue scoring (batch) | 150 | On scan |
| Triage | AI priority reorder | 0 | After scores |
| Triage | Explain issue | 60 | On demand |
| Triage | Fix SQL preview (zero tokens) | 0 | After dry-run |
| Workflows | AI Suggest (3 workflows) | 1400 | On demand |
| Workflows | AI Workflow Assistant chat | 800 | On demand |
| Workflows | AI check generator | 400 | On demand |
| Workflows | Failure pattern detection | 0 | Always |
| Dataflows | AI Builder chat panel | 800 | On demand |
| Dataflows | AI checks in detail modal | 400 | On demand |
| Dataflows | Last-run commentary | 80 | After run |
| Query | Proactive suggestions | 0 | Always |
| Query | AI SQL generate | 200 | On demand |
| Query | AI explain results | 120 | On demand |
| Query | Anomaly detection | 0 | After query |
| Results | Run insight | 60 | On demand |
| Results | Compare runs | 100 | On demand |
| Results | New-failure diff | 0 | Always |
| Results | Stakeholder explain | 100 | On demand |
| Scheduler | Conflict detection | 0 | Always |
| Scheduler | Schedule recommendation | 0 | Always |
| Scheduler | AI name suggest | 0 | On demand |
| Demo Validation | Staleness explanation | 0 | Always |
| Demo Validation | Per-account health | 0 | Always |
| Demo Validation | AI date column suggest | 200 | On demand |
| Configure | Diagnostics | 0 | On demand |
| Configure | SLA suggestions | 0 | On demand |
| Configure | Connection string validator | 0 | On demand |
| AdsSopTab | Gate decision assistant | 60/gate | Auto on pending |
| Activity | Weekly pattern summary | 180 | On demand |
| Approvals | Risk assessment | 0 | Always |
| Evals | Generate eval cases | 200 | On demand |
| Monitor | AI checks on add table | 400 | On demand |
| Upload | Data profiling | 0 | On parse |
| FloatingAssistant | Agent with 14 tools | varies | On demand |
| Onboarding | Setup wizard | 900 | First visit only |
| Proactive cron | Anomaly detection (backend) | 0 | Hourly |
| Digest | AI narrative on send | 120 | On send |

### Agent Tools (14 total)
`run_sql, navigate, run_workflow, get_workflow_status, get_dataflows, run_dataflow, get_recent_results, get_kpis, get_alerts, get_triage_issues, get_schema, create_dataflow, get_schedules, get_sla_status, get_demo_status, create_schedule`

---

## Backend Key Endpoints
```
GET  /api/schema                     — Redshift schema
GET  /api/kpis                       — revenue/orders/inventory KPIs
GET  /api/anomaly/proactive          — proactive alert list
POST /api/ai/chat                    — ChatRequest → OpenAI GPT-4o
POST /api/ai/validate-sql            — SQL validation via EXPLAIN
POST /api/ai/agent                   — Agentic tool loop
POST /api/ai/analyse-run             — Workflow run analysis
GET  /api/sla/history                — SLA per-day history
GET  /api/sla/suggest                — AI SLA threshold suggestions
GET  /api/custom-workflows           — List workflows
POST /api/custom-workflows/save/v2   — Save workflow
DELETE /api/custom-workflows/{id}    — Delete workflow (tombstone)
GET  /api/workflow-results/full      — Re-execute check SQL, paginated
POST /api/demo/config                — Save demo validation config
GET  /api/demo/mws-tables            — List mws schema tables
POST /api/demo/ai-suggest            — AI suggest date column for table
GET  /api/dataflows                  — List dataflows
POST /api/dataflows/save             — Save dataflow
GET  /api/schedules                  — List schedules
POST /api/digest/send                — Send Slack digest + AI narrative
```

### Background Cron (60s loop)
`custom_workflow_cron_check → sop_auto_trigger → schedules_cron_check → _proactive_anomaly_check`

---

## Known Bugs (unfixed as of last session)

### 🔴 Dark Mode Breaks
1. **FloatingAssistant** — entire panel hardcoded `#FFFFFF`/`#0D1117`/`#E8ECF0` → white box in dark mode
2. **Sticky table headers** — `background:"#FAFBFF"` → white stripe in dark mode on scroll
3. **Alternating table rows** — `i%2===1?"#FAFBFF":"transparent"` → visible in dark mode

### 🔴 Functional Bugs
4. **SHORTCUT_MAP broken** — wrong key mappings, missing demo/results/scheduler/dataflows keys, maps `"5":"activity"` and `"6":"chat"` which don't exist as tabs
5. **11× window.confirm()** — `ConfirmModal` component exists but not yet wired to these calls
6. **Triage Data Preview** — doesn't auto-refresh when table selector changes
7. **WorkflowBuilder dbSchema** — receives prop AND calls useSchema() — prop takes precedence but if empty, hook ignored

### 🟡 Minor
8. Demo Validation — `loadMwsTables()` called twice on "Add Table" click  
9. AiWorkflowAssistant / AiDataflowAssistant — no loading indicator during `onSave` round-trip

---

## Recent Visual Changes Applied
- Content bg: `#F8FAFC` → `#EEF2F7` (cards now float)
- Card: `box-shadow: 0 1px 3px rgba(0,0,0,0.06), 0 1px 2px rgba(0,0,0,0.04)`
- Light theme border: `#F0F0F0` → `#E4E7EC`, border2: `#D0D5DD`
- Tab bars: pill style with track background
- KPI cards: colour dot + 34px number + trend line
- Page titles: unified 24px/800/-0.03em
- Severity badges: solid fills (GitHub-style)
- Modal backdrops: `backdrop-filter: blur(4-6px)`
- Dark/Midnight themes added
- Glassmorphism on cards in dark mode
- Sidebar: gradient + drop shadow + left-border active indicator
- `ConfirmModal` + `useConfirm()` hook added (not yet replacing confirm() calls)
- CommandPalette: all 11 tabs, fully theme-aware
- Notification drawer: fully theme-aware
- Toast: enter animation, theme-aware background
- BellButton: lights up accent when unread > 0
- `useDebounce` hook, applied to WorkflowsTab search
- `Skeleton` component added (shimmer)
- `GLOBAL_CSS` injected once via useEffect (not re-diffed)
- `contain: layout style` on `<main>`

---

## Development Rules (Zubair's preferences)
1. **grep-first** — never read full file, always slice with line numbers
2. **Batch patches** via Python script — one `file.read()` / `file.write()` per script
3. **Babel validate** after every JSX change: `node -e "parser.parse(..., {plugins:['jsx']})"` at `/tmp/parse_test`
4. **Python ast.parse** after every backend change
5. **Single output files** — always copy to `/mnt/user-data/outputs/`
6. **No ON CONFLICT DO NOTHING** — Redshift doesn't support it, use DELETE+INSERT
7. **No PRIMARY KEY constraints** in Redshift tables — they parse but aren't enforced and can cause insert failures
8. Token economy: rule-based (0 tokens) wherever possible, AI only for genuine language tasks

---

## Next Session — Recommended First Actions
1. Fix SHORTCUT_MAP (5 min, pure JS)
2. Fix FloatingAssistant dark mode (grep hardcoded colours, replace with T.card/T.text etc)
3. Fix sticky table header + alternating row hardcoded whites
4. Wire `useConfirm()` to replace all 11 `window.confirm()` calls
5. Continue with any new features

