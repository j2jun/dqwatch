# dqwatch

A no-code data quality platform for people who aren't data engineers.

Connect a source once (a folder you drop CSVs into, a CSV link such as a published Google Sheet,
or a public API). dqwatch then ingests new and changed data on its own and runs the checks you
built by clicking. An AI assistant explains concepts and failures and suggests checks; you approve each one.

**The core promise: checks never affect the job.** Data always lands. A failing check, a check
that crashes, an email server that's down, or an assistant that's offline is recorded and
reported. None of them stops, slows, or rolls back a load.

## How it works

```
             sensor (every 30 s, one run per source per schedule slot)
                │
   Dagster ─► ingest_and_check job ─► fetch ─► stage ─► load ──commit──► checks ─► alert
                                       │        │                     │          │
                               folder / URL /   cast to schema,       rules      email on
                               Open-Meteo       keep bad values       compiled   pass → fail
                                                in _raw, set aside    to SQL     (error only)
                                                rows with no key
   Web app (FastAPI + HTMX) ─► setup wizard, rule builder, dashboard, AI chat
```

| Piece | Tech | Notes |
|---|---|---|
| Orchestration | Dagster | One generic job; a sensor reads sources from SQLite. No per-source code generation. |
| User data | DuckDB | One table per source. Only Dagster writes it, one run at a time (DuckDB has a single writer). |
| Metadata | SQLite | Sources, rules (plain JSON), runs, check history. The web app only touches this. |
| Checks | own engine | `not_null`, `unique`, `range`, `allowed`, `pattern` (email/date/digits), `row_count`, `freshness`, plus built-in `schema`, `duplicate_keys`, `fetch`. |
| AI | any OpenAI-compatible API | Ollama by default (free, local); Groq/Gemini free tiers by changing one setting. |
| Alerts | SMTP | Optional; Gmail app password works. |

**Write modes** (chosen per source):
- **Update by key** (default): rows with the same ID are updated, new ones added.
- **Append**: exact-duplicate rows are skipped, so a sheet that re-sends everything doesn't double up.
- **Replace**: the newest file becomes the whole table.

**When data doesn't fit the schema**, the load still completes:
- A value that can't be read as its type is stored as empty, and the original is kept in `_raw`.
- A missing column is stored as empty. New columns are reported but not added.
- Rows with no key are set aside under "Needs attention".
- The built-in `schema` check reports all of the above.

## Quickstart

```bash
cp .env.example .env            # optional: email + AI settings
docker compose up -d --build
docker compose exec ollama ollama pull llama3.2:3b   # one-time, a few GB
```

- dqwatch: http://localhost:8000
- Dagster UI (runs, logs): http://localhost:3000

Your data lives in `./data` next to this file. Drop more CSVs for a folder source into
`./data/inbox/<folder name>/` and they're picked up within a minute.

**Demo, about 2 minutes:**
1. Go to **Add a source** and pick **A folder I drop CSV files into**.
   Upload a CSV with one bad email in it.
   Confirm the columns, then pick *Update by key*.
2. On the source page, add the check **pattern · email**, set to **Email me**.
3. Within a minute, Dagster shows a green run. Both rows are stored, and the check shows `fail`
   with the bad row as its sample.
4. Add a second source with **Weather data**: latitude 40.7, longitude -74.0, key column `time`.
   Hourly forecast rows load on a schedule.
5. On the source page, click **Suggest checks** to get AI suggestions, then add the ones you like.

## Development

```bash
python3.12 -m venv .venv && .venv/bin/pip install -e '.[dev]'
.venv/bin/pytest -q          # fully offline: no real LLM, email, or network calls
```

Run without Docker:

```bash
export DQ_META=data/meta.sqlite DQ_DATA=data/data.duckdb DQ_INBOX=data/inbox DAGSTER_HOME=$PWD/.dagster
mkdir -p data/inbox .dagster && cp dagster.yaml .dagster/
.venv/bin/uvicorn dqwatch.web.app:app --port 8000 &
.venv/bin/dagster dev -m dqwatch.orchestration -p 3000
```

## Limits and security notes

- **Local, single-user tool with no login.** URL sources fetch whatever address you enter,
  including addresses on your own network. That's by design for local use. Before deploying it
  publicly, add authentication and block private/loopback addresses for URL sources.
- Form posts from other sites are refused (Origin/Referer must match), the app only answers to
  the host names in `DQ_ALLOWED_HOSTS` (default `localhost,127.0.0.1`, which blocks DNS rebinding),
  and ports are bound to `127.0.0.1` so other machines can't reach it.
- The assistant waits up to 2 minutes, because the first answer after starting Ollama can be
  slow while the model loads on a CPU-only machine.
- Downloads are capped at 50 MB and 30 seconds. API sources are limited to `api.open-meteo.com`.
- Runs are serialized (one at a time) because DuckDB allows only one writer. If you need more
  throughput, use one DuckDB file per source.
- If you switch the assistant to a hosted provider, sample rows from your data are sent to that
  provider. The chat panel says so when that's the case.
