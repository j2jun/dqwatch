"""AI assistant over any OpenAI-compatible endpoint (Ollama by default; Groq/Gemini work too)."""
import json
import os
import urllib.parse
import urllib.request
from dataclasses import dataclass

from dqwatch import rules
from dqwatch.models import Source

LOCAL_HOSTS = {"localhost", "127.0.0.1", "ollama", "::1"}

INTRO = ("You are the helper inside dqwatch, a data quality tool. The person you're talking to is "
         "not a data engineer: use plain language, short answers, and explain any term you use. "
         "You can explain concepts, explain why a check failed, and suggest checks, but you never "
         "change settings yourself; the user adds suggestions by clicking.")

SUGGEST = ("Suggest up to 5 data quality checks for this dataset. Reply with ONLY a JSON array. "
           "Each item is one of: "
           '{"type":"not_null","column":C}, {"type":"unique","column":C}, '
           '{"type":"range","column":C,"min":N,"max":N}, {"type":"allowed","column":C,"values":[...]}, '
           '{"type":"pattern","column":C,"preset":"email"|"date"|"digits"}, {"type":"row_count","min":N}, '
           '{"type":"freshness","column":C,"max_age_hours":N}. '
           'Add "why": a one-sentence plain-language reason. Use only the columns listed.')


@dataclass
class LlmConfig:
    base_url: str
    api_key: str
    model: str


class LlmUnavailable(Exception):
    pass


def llm_from_env() -> LlmConfig:
    return LlmConfig(os.environ.get("DQ_LLM_BASE_URL", "http://ollama:11434/v1"),
                     os.environ.get("DQ_LLM_API_KEY", ""), os.environ.get("DQ_LLM_MODEL", "llama3.2:3b"))


def is_hosted(cfg: LlmConfig) -> bool:
    return urllib.parse.urlparse(cfg.base_url).hostname not in LOCAL_HOSTS


def complete(messages: list[dict], cfg: LlmConfig, timeout: float = 60) -> str:
    req = urllib.request.Request(
        cfg.base_url.rstrip("/") + "/chat/completions",
        data=json.dumps({"model": cfg.model, "messages": messages}).encode(),
        headers={"Content-Type": "application/json",
                 **({"Authorization": f"Bearer {cfg.api_key}"} if cfg.api_key else {})})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read())["choices"][0]["message"]["content"]
    except Exception as e:
        raise LlmUnavailable(str(e)) from e


def system_prompt(source: Source | None, rules_: list[dict], history: list[dict]) -> str:
    parts = [INTRO]
    if source:
        cols = ", ".join(f"{c.name} ({c.type})" for c in source.columns)
        parts.append(f"Dataset '{source.name}' with columns: {cols}. Write mode: {source.write_mode}"
                     + (f", key column: {source.key_column}." if source.key_column else "."))
        parts.append("Sample rows: " + json.dumps(source.sample[:10], default=str))
        parts.append("Current checks: " + json.dumps(rules_))
        recent = [{k: h.get(k) for k in ("rule_id", "status", "detail", "failing_count")} for h in history[:10]]
        parts.append("Recent check results (newest first): " + json.dumps(recent, default=str))
    return "\n\n".join(parts)


def _first_json_array(text: str):
    start = text.find("[")
    if start < 0:
        return []
    try:
        value, _ = json.JSONDecoder().raw_decode(text[start:])
    except ValueError:
        return []
    return value if isinstance(value, list) else []


def suggest_rules(source: Source, existing: list[dict], cfg: LlmConfig, complete=complete) -> list[dict]:
    reply = complete([{"role": "system", "content": system_prompt(source, existing, [])},
                      {"role": "user", "content": SUGGEST}], cfg)
    seen = {(r.get("type"), r.get("column")) for r in existing}
    out = []
    for item in _first_json_array(reply):
        if not isinstance(item, dict):
            continue
        why = str(item.pop("why", ""))[:300]
        item.pop("id", None)
        try:
            rule = rules.validate_rule(item, source.columns)
        except (ValueError, TypeError):
            continue
        if (rule["type"], rule.get("column")) in seen:
            continue
        seen.add((rule["type"], rule.get("column")))
        out.append({**rule, "why": why})
    return out
