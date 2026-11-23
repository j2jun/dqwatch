"""Web UI. Talks only to the SQLite metadata store; DuckDB is written by Dagster runs alone."""
import json
import os
import re
import urllib.parse
from pathlib import Path

from fastapi import FastAPI, Form, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from dqwatch import fetch, llm, meta, rules, schema
from dqwatch.models import Column, Source

TEMPLATES = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))
FOLDER_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
SCHEDULES = {15: "Every 15 minutes", 60: "Every hour", 1440: "Every day"}
MODES = {"upsert": "Update by key", "append": "Append new rows", "replace": "Replace everything"}


def _num(v: str):
    v = (v or "").strip()
    if not v:
        return None
    for cast in (int, float):
        try:
            return cast(v)
        except ValueError:
            pass
    raise ValueError(f"{v!r} isn't a number.")


def _rule_from_form(form) -> dict:
    if form.get("rule_json"):
        rule = json.loads(form["rule_json"])
        rule.pop("why", None)
        rule.pop("id", None)
        return rule
    t = form.get("type", "")
    rule = {"type": t, "severity": form.get("severity") or "error"}
    if t != "row_count":
        rule["column"] = form.get("column") or ("_loaded_at" if t == "freshness" else "")
    if t == "range":
        rule["min"], rule["max"] = _num(form.get("min")), _num(form.get("max"))
    elif t == "allowed":
        rule["values"] = [v.strip() for v in (form.get("values") or "").split(",") if v.strip()]
    elif t == "pattern":
        rule["preset"] = form.get("preset")
    elif t == "row_count":
        rule["min"] = _num(form.get("min"))
    elif t == "freshness":
        rule["max_age_hours"] = _num(form.get("max_age_hours"))
    return rule


def location_error(kind: str, location: str) -> str:
    """Server-side check of a source's kind/location (the runtime fetcher enforces the same)."""
    if kind == "folder":
        return "" if FOLDER_RE.match(location) else "Folder names can use letters, numbers, - and _ only."
    if kind == "url":
        return "" if location.startswith(("http://", "https://")) else "Links must start with http:// or https://."
    if kind == "api":
        ok = urllib.parse.urlparse(location).hostname == fetch.API_HOST
        return "" if ok else "The weather source must use the Open-Meteo API."
    return "Pick a source type."


def create_app(meta_path: str, inbox_root: str, llm_cfg: llm.LlmConfig, complete=llm.complete) -> FastAPI:
    app = FastAPI(title="dqwatch")
    app.state.llm_cfg, app.state.complete = llm_cfg, complete

    def db():
        return meta.connect(meta_path)

    def page(request, name, status_code=200, **ctx):
        ctx.setdefault("hosted", llm.is_hosted(llm_cfg))
        return TEMPLATES.TemplateResponse(request, name, ctx, status_code=status_code)

    def source_page(request, sid: int, error: str = ""):
        con = db()
        src = meta.get_source(con, sid)
        history = meta.results_history(con, sid, 500)
        strips: dict[str, list[dict]] = {}
        for h in reversed(history):
            strips.setdefault(h["rule_id"], []).append(h)
        return page(request, "source.html", source=src, rules=meta.list_rules(con, sid),
                    strips={k: v[-30:] for k, v in strips.items()}, attention=meta.list_attention(con, sid)[:50],
                    error=error, modes=MODES, schedules=SCHEDULES, rule_types=sorted(rules.RULE_TYPES),
                    patterns=rules.PATTERNS)

    app.state.page, app.state.db, app.state.source_page = page, db, source_page

    @app.get("/sources/new", response_class=HTMLResponse)
    def new_source(request: Request):
        return page(request, "sources_new.html", error="", form={})

    @app.post("/sources/preview", response_class=HTMLResponse)
    async def preview_source(request: Request, name: str = Form(...), kind: str = Form(...),
                             location: str = Form(""), lat: str = Form(""), lon: str = Form(""),
                             upload: UploadFile | None = None):
        form = {"name": name, "kind": kind, "location": location, "lat": lat, "lon": lon}

        def fail(msg):
            return page(request, "sources_new.html", error=msg, form=form)

        if kind == "folder":
            if not FOLDER_RE.match(location):
                return fail("Folder names can use letters, numbers, - and _ only.")
            folder = Path(inbox_root) / location
            folder.mkdir(parents=True, exist_ok=True)
            if upload and upload.filename:
                fname = os.path.basename(upload.filename)
                if not fname.lower().endswith(".csv"):
                    return fail("Please upload a .csv file.")
                (folder / fname).write_bytes(await upload.read())
        elif kind == "api":
            try:
                location = fetch.open_meteo_url(float(lat), float(lon))
            except ValueError:
                return fail("Enter a latitude and longitude as numbers, e.g. 40.7 and -74.0.")
            form["location"] = location
        elif kind != "url":
            return fail("Pick a source type.")
        probe = Source(None, name, kind, location, 60, "replace", None, [])
        try:
            items = fetch.fetch(probe, inbox_root)
            if not items:
                return fail(f"No CSV files in the '{location}' folder yet. Upload one to get started.")
            data = items[-1][1]
            columns, sample = schema.infer_columns(data), schema.preview(data)
        except (fetch.FetchError, ValueError) as e:
            return fail(str(e))
        return page(request, "source_preview.html", form=form, columns=columns, sample=sample,
                    types=list(schema.TYPES), modes=MODES, schedules=SCHEDULES, sample_json=json.dumps(sample))

    @app.post("/sources", response_class=HTMLResponse)
    async def create_source(request: Request):
        f = await request.form()
        names, types = f.getlist("col_name"), f.getlist("col_type")
        mode, key = f.get("write_mode", "upsert"), (f.get("key_column") or None)
        cols = [Column(n, t) for n, t in zip(names, types) if t in schema.TYPES]
        error = (location_error(f.get("kind", ""), f.get("location", ""))
                 or ("Pick how updates should be stored." if mode not in MODES else "")
                 or ("'Update by key' needs a key column: pick the column that identifies a row."
                     if mode == "upsert" and key not in names else "")
                 or ("No columns to store." if not cols else ""))
        if error:
            return page(request, "sources_new.html", error=error, form=dict(f))
        kind = f.get("kind")
        minutes = 1 if kind == "folder" else int(f.get("schedule_minutes") or 60)
        try:
            sample = json.loads(f.get("sample") or "[]")
        except ValueError:
            sample = []
        sid = meta.create_source(db(), Source(None, f.get("name") or "Untitled", kind, f.get("location", ""),
                                              minutes, mode, key if mode == "upsert" else None, cols, sample))
        return RedirectResponse(f"/sources/{sid}", status_code=303)

    @app.get("/sources/{sid}", response_class=HTMLResponse)
    def show_source(request: Request, sid: int):
        return source_page(request, sid)

    @app.get("/sources/{sid}/rule-form", response_class=HTMLResponse)
    def rule_form(request: Request, sid: int, type: str = "not_null"):
        return page(request, "rule_form.html", source=meta.get_source(db(), sid), type=type,
                    patterns=rules.PATTERNS)

    @app.post("/sources/{sid}/rules", response_class=HTMLResponse)
    async def add_rule(request: Request, sid: int):
        con = db()
        src = meta.get_source(con, sid)
        try:
            rule = rules.validate_rule(_rule_from_form(await request.form()), src.columns)
        except ValueError as e:
            return source_page(request, sid, error=str(e))
        meta.add_rule(con, sid, rule)
        return RedirectResponse(f"/sources/{sid}", status_code=303)

    @app.post("/sources/{sid}/rules/{rule_id}/delete")
    def delete_rule(sid: int, rule_id: str):
        meta.delete_rule(db(), sid, rule_id)
        return RedirectResponse(f"/sources/{sid}", status_code=303)

    return app


def _app_from_env() -> FastAPI:
    return create_app(os.environ.get("DQ_META", "data/meta.sqlite"), os.environ.get("DQ_INBOX", "data/inbox"),
                      llm.llm_from_env())


app = _app_from_env()  # connects lazily, per request
