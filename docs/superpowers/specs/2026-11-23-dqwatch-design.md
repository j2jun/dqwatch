# dqwatch — No-code data quality platform

Date: 2026-11-23 · Status: draft, pending approval

## Intent

A portfolio project showing data engineering skill: a platform where people who are
**not data engineers** (analysts, ops staff) connect a data source once, describe how it
should be stored, and build data quality checks by clicking, with no code. After setup,
ingestion and checks run automatically on a schedule. Checks **never affect the job**:
data always lands, and bad data is flagged and reported, never blocked or rolled back.

An AI assistant guides brand-new users: explains concepts, explains failures, and
suggests checks the user approves.

## Goals

- Self-serve source setup: file sources (watched folder, public CSV URL such as a
  published Google Sheet) and scheduled public API pulls.
- After setup, fully automatic: new or changed data is detected and ingested with no user action.
- Per-source write mode, chosen at setup: **replace**, **append**, or **update by key**
  (default: update by key; the user picks the ID column).
- No-code rule builder; rules are stored as plain JSON (readable, editable, versionable).
- Non-blocking checks with history and trends ("this rule started failing Tuesday").
- Dashboard + email alerts on failures.
- AI chat: explains in plain language and proposes checks from a data sample; the user
  accepts each one. The AI never changes configuration on its own.
- Zero cost to run: `docker compose up` locally, no paid services.

## Non-goals

- Multi-tenant SaaS, user accounts, or login (single local workspace). *My call:
  portfolio/local scope; add auth if it's ever deployed publicly.*
- Transformations / modeling (dbt-style). Data is stored as ingested, typed per schema.
- Data repair or auto-quarantine. Checks report; they don't fix or remove data.
- Private/authenticated connectors (OAuth Drive, databases, S3). Public URLs, folder, public APIs only.
- Real-time streaming.

## Anti-goals (failure even if it "works")

- A check failure, a check crash, the email server being down, or the AI being down
  causes a load to fail, stall, or roll back.
- A non-engineer needs to write SQL, YAML, or Python to do anything in the main flow.
- The AI silently edits sources or rules.
- Any step requires a paid API or paid infrastructure to run the project.

## Constraints

- Zero cost; local-first; runs with `docker compose up` (app, Dagster, Ollama).
- Tests never call a real LLM or send real email (fakes/stubs); no-spend by default.
- Python stack.

## Approaches considered

1. **Lean single app** — FastAPI + HTMX + DuckDB/SQLite + in-app scheduler.
   Fewest parts; no recognized orchestrator.
2. **Lean app + Dagster orchestration** — same app, Dagster runs the jobs. **Chosen.**
3. **Lean app + Airflow** — most résumé-recognizable, heaviest to run locally, no extra
   capability needed here.
4. (Rejected earlier) Soda Core / Great Expectations as check engine — quirks leak into a
   no-code UI, harder error messages for non-engineers; GX is heavy. Own engine kept.

**Why 2:** Dagster is modern and trending, its run UI helps debugging, and it fits the
non-blocking-checks story. The user chose it after comparing on adoption and trendiness.

## Decided design

### Components

| Component | Tech | Role |
|---|---|---|
| Web app | FastAPI, server-rendered templates + HTMX | Source setup wizard, rule builder, dashboard, AI chat |
| Metadata store | SQLite | Sources, schemas, rules (JSON), runs, check results, file fingerprints |
| Data store | DuckDB (file) | One table per source holding the user's data |
| Orchestrator | Dagster (OSS) | One generic `ingest_and_check` job; a sensor reads SQLite and launches runs for sources that are due |
| LLM | Any OpenAI-compatible endpoint (base URL + key + model in settings) | Default: Ollama local; can switch to Groq/Gemini free tier |
| Email | SMTP (e.g. Gmail app password) | Failure alerts; optional — if unset, dashboard only |

### Dagster shape (decided)

- **No per-source code generation.** A single generic job takes a source ID as run config.
- A sensor ticks (e.g. every 30 s), reads sources from SQLite, and yields a RunRequest
  for each source whose schedule is due. Run key = source ID + due time, to avoid duplicates.
- Job steps: `fetch` → `detect_change` → `load` → `run_checks` → `notify`.
  `run_checks` and `notify` run **after the load commits**; their failures are caught,
  recorded (check status `error`), and never fail the load step or undo it.

### Sources

- **Watched folder:** user picks a subfolder under a mounted `inbox/` dir; new or
  overwritten files are picked up on the next sensor tick.
- **CSV URL:** polled on the user's schedule (e.g. every 15 min / hourly / daily).
- **Public API:** built-in demo connector (Open-Meteo weather, no key needed) on a
  schedule; JSON flattened to rows. *My call on the demo source.*
- **Change detection:** content hash of the fetched file/response; unchanged → skip load
  and checks, record "no change".
- **Schema at setup:** platform samples the source, infers column names and types, and
  the user confirms or edits them. The user picks the write mode and, for update-by-key, the key column.

### Checks (non-blocking)

- Rule types (initial set): not null, unique, value in range, allowed values,
  matches pattern (preset choices like email/date, no regex typing required), row count
  at least N, freshness (data newer than X).
- Each rule is JSON, e.g. `{"type": "range", "column": "amount", "min": 0, "max": 10000, "severity": "warn"}`, compiled to a SQL query against DuckDB.
- Checks run against the rows touched by the latest load (append/update-by-key) or the
  whole table (replace), so results describe what just arrived.
- Each result stores: pass/fail/error, failing-row count, and a small sample of failing rows.
- History retained for trend view on the dashboard.

### Alerts

- Email on a rule going from pass → fail (and a daily digest is out of scope initially),
  to avoid one email per run for a long-running failure. *My call.*
- Email send failure is logged and shown on the dashboard; it never fails the job.

### AI assistant

- Chat panel available everywhere; context = current page's source schema, a small row
  sample, and recent check results.
- Can: explain concepts, explain failures, walk through setup, propose rules.
- Proposed rules appear as cards with **Add** / **Dismiss**; nothing is saved without a click.
- UI notes that switching to a hosted model sends the sample rows to that provider.
- If the LLM is unreachable, the chat shows a friendly message; the rest of the platform is unaffected.
- Model: a small current instruction-tuned model, chosen and sanity-checked at build time.

### Schema mismatches (decided: "store what fits, flag the rest")

- A value that can't be cast to its column type → stored as NULL in the typed column;
  the original text is kept in a per-row raw JSON column (`_raw`), so nothing is lost.
- Missing column → NULL for that column. New column → not added to the table, reported.
- Rows with a null key (update-by-key) → set aside in a per-source "needs attention"
  table, visible in the UI.
- All of the above are recorded by a built-in **schema** check that behaves like any
  other rule on the dashboard and in email. The load always completes.
- Empty or unparseable file → nothing to load; recorded as a fetch error (see below).

## Builder-check calls (my calls, from the reviewer's questions)

- **Freshness & fetch failures:** freshness rules run on every scheduled tick, including
  "no change" ticks. A failed fetch (URL down, API error, unreadable file) is recorded as a
  run error, shown on the dashboard, and emailed on transition (ok → failing), like rules.
- **Update by key:** duplicate keys within one batch → last row wins, and a built-in
  "duplicate keys in batch" result is recorded. Keys missing from a later file stay in the
  table (upsert, not sync); users wanting sync pick replace.
- **Rule scope:** row-level rules (not null, range, allowed values, pattern) and row count
  run on the latest batch; unique and freshness run on the whole table.
- **Watched folder:** every file in the subfolder is its own load, fingerprinted
  separately. In replace mode, the newest file replaces the table.
- **Append:** exact-duplicate rows already in the table are skipped (row hash), so a
  sheet that re-sends everything doesn't double up. The wizard recommends update-by-key
  for CSV URLs.
- **Severity:** `warn` → dashboard only; `error` → dashboard + email.

## Left to the builder

- Exact page layouts and styling; the dashboard chart library.
- Exact module layout, SQLite schema details, migrations approach.
- How the inferred-type mapping and JSON flattening handle edge cases.
- Sensor interval, retention period for check history and failing-row samples.
- Test strategy beyond: rule→SQL compile tests, write-mode tests, a non-blocking guarantee
  test (a crashing check and a failing SMTP still leave the load committed), all offline.
