import json

import pytest
from fastapi.testclient import TestClient

from dqwatch import meta
from dqwatch.llm import LlmConfig
from dqwatch.web.app import create_app

CSV = b"id,email,amount\n1,a@x.com,5.5\n2,b@x.com,7\n"


@pytest.fixture
def ctx(tmp_path):
    mp, inbox = str(tmp_path / "m.sqlite"), tmp_path / "inbox"
    inbox.mkdir()
    app = create_app(mp, str(inbox), LlmConfig("http://ollama:11434/v1", "", "m"))
    return {"client": TestClient(app), "meta": mp, "inbox": inbox}


def _create(ctx):
    c = ctx["client"]
    r = c.post("/sources/preview", data={"name": "Orders", "kind": "folder", "location": "orders"},
               files={"upload": ("first.csv", CSV, "text/csv")})
    assert r.status_code == 200
    return r


def test_wizard_folder_upload_creates_source(ctx):
    r = _create(ctx)
    assert "integer" in r.text and "number" in r.text and "a@x.com" in r.text
    assert (ctx["inbox"] / "orders" / "first.csv").read_bytes() == CSV
    r = ctx["client"].post("/sources", data={
        "name": "Orders", "kind": "folder", "location": "orders", "write_mode": "upsert", "key_column": "id",
        "schedule_minutes": "60", "col_name": ["id", "email", "amount"], "col_type": ["integer", "text", "number"],
        "sample": json.dumps([{"id": "1"}])})
    assert r.status_code == 200 and "Orders" in r.text and "amount" in r.text
    src = meta.list_sources(meta.connect(ctx["meta"]))[0]
    assert src.schedule_minutes == 1 and src.key_column == "id"  # folders are always checked every minute


def test_upsert_requires_key(ctx):
    r = ctx["client"].post("/sources", data={
        "name": "O", "kind": "url", "location": "http://x/a.csv", "write_mode": "upsert", "key_column": "",
        "schedule_minutes": "60", "col_name": ["id"], "col_type": ["integer"], "sample": "[]"})
    assert "key column" in r.text.lower()
    assert meta.list_sources(meta.connect(ctx["meta"])) == []


def _source(ctx):
    con = meta.connect(ctx["meta"])
    from dqwatch.models import Column, Source
    return meta.create_source(con, Source(None, "Orders", "folder", "orders", 1, "upsert", "id",
                                          [Column("id", "integer"), Column("amount", "number")]))


def test_add_rule_and_validation_error(ctx):
    sid = _source(ctx)
    c = ctx["client"]
    r = c.post(f"/sources/{sid}/rules", data={"type": "range", "column": "amount", "min": "0", "max": "100",
                                              "severity": "warn"})
    assert r.status_code == 200
    saved = meta.list_rules(meta.connect(ctx["meta"]), sid)
    assert saved[0]["min"] == 0 and saved[0]["max"] == 100 and saved[0]["severity"] == "warn"
    r = c.post(f"/sources/{sid}/rules", data={"type": "range", "column": "amount", "min": "", "max": ""})
    assert "Set a minimum" in r.text
    assert len(meta.list_rules(meta.connect(ctx["meta"]), sid)) == 1


def test_add_rule_from_suggestion_json(ctx):
    sid = _source(ctx)
    ctx["client"].post(f"/sources/{sid}/rules",
                       data={"rule_json": json.dumps({"type": "unique", "column": "id", "why": "ids repeat"})})
    rule = meta.list_rules(meta.connect(ctx["meta"]), sid)[0]
    assert rule["type"] == "unique" and "why" not in rule


def test_delete_rule(ctx):
    sid = _source(ctx)
    rid = meta.add_rule(meta.connect(ctx["meta"]), sid, {"type": "unique", "column": "id"})
    ctx["client"].post(f"/sources/{sid}/rules/{rid}/delete")
    assert meta.list_rules(meta.connect(ctx["meta"]), sid) == []


def test_bad_url_friendly_error(ctx):
    r = ctx["client"].post("/sources/preview", data={"name": "x", "kind": "url", "location": "http://127.0.0.1:9/a.csv"})
    assert r.status_code == 200 and "reach" in r.text


def test_bad_folder_name_rejected(ctx):
    r = ctx["client"].post("/sources/preview", data={"name": "x", "kind": "folder", "location": "../etc"},
                           files={"upload": ("a.csv", CSV, "text/csv")})
    assert "letters, numbers" in r.text
    assert not (ctx["inbox"].parent / "etc").exists()


def test_rule_form_fields(ctx):
    sid = _source(ctx)
    r = ctx["client"].get(f"/sources/{sid}/rule-form", params={"type": "pattern"})
    assert 'name="preset"' in r.text and "email" in r.text
