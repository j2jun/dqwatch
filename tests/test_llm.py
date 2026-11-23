import json

import pytest

from dqwatch import llm
from dqwatch.models import Column, Source

SRC = Source(1, "orders", "folder", "orders", 1, "upsert", "id",
             [Column("id", "integer"), Column("email", "text")], [{"id": "1", "email": "a@x.com"}])
CFG = llm.LlmConfig("http://ollama:11434/v1", "", "m")


def _fake(reply):
    calls = []

    def complete(messages, cfg):
        calls.append(messages)
        return reply
    return complete, calls


def test_suggest_valid_kept():
    reply = 'Here are some ideas:\n[{"type": "unique", "column": "id"}, {"type": "not_null", "column": "email"}]\nEnjoy!'
    complete, calls = _fake(reply)
    got = llm.suggest_rules(SRC, [], CFG, complete=complete)
    assert [(r["type"], r["column"]) for r in got] == [("unique", "id"), ("not_null", "email")]
    assert "email" in json.dumps(calls[0])  # schema went into the prompt


def test_suggest_garbage():
    assert llm.suggest_rules(SRC, [], CFG, complete=_fake("sure! here you go")[0]) == []
    assert llm.suggest_rules(SRC, [], CFG, complete=_fake("[{not json")[0]) == []
    assert llm.suggest_rules(SRC, [], CFG, complete=_fake('[{"type": "not_null", "column": "nope"}, 5]')[0]) == []


def test_suggest_skips_existing():
    complete, _ = _fake('[{"type": "unique", "column": "id"}]')
    assert llm.suggest_rules(SRC, [{"type": "unique", "column": "id", "id": "x"}], CFG, complete=complete) == []


def test_is_hosted():
    assert not llm.is_hosted(CFG)
    assert not llm.is_hosted(llm.LlmConfig("http://localhost:11434/v1", "", "m"))
    assert llm.is_hosted(llm.LlmConfig("https://api.groq.com/openai/v1", "k", "m"))


def test_complete_unreachable():
    with pytest.raises(llm.LlmUnavailable):
        llm.complete([{"role": "user", "content": "hi"}], llm.LlmConfig("http://127.0.0.1:9", "", "m"), timeout=2)


def test_system_prompt_has_context():
    p = llm.system_prompt(SRC, [{"type": "unique", "column": "id"}],
                          [{"rule_id": "schema", "status": "fail", "detail": "missing columns: amount"}])
    assert "orders" in p and "email" in p and "a@x.com" in p and "missing columns: amount" in p
    assert "not a data engineer" in llm.system_prompt(None, [], [])


def test_suggest_null_content():
    assert llm.suggest_rules(SRC, [], CFG, complete=_fake(None)[0]) == []


def test_suggest_skips_bracket_before_array():
    reply = 'Sure [see below]: [{"type": "unique", "column": "id"}]'
    assert [r["type"] for r in llm.suggest_rules(SRC, [], CFG, complete=_fake(reply)[0])] == ["unique"]
