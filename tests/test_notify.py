from dqwatch import notify
from dqwatch.models import CheckResult

CFG = notify.SmtpConfig("smtp.example.com", 587, "u", "p", "me@example.com")


def test_subject_and_body():
    sent = []
    fails = [CheckResult("r1", "fail", "error", 3, detail="3 empty emails"),
             CheckResult("fetch", "error", "error", detail="couldn't reach")]
    notify.send_alert("orders", fails, CFG, send=lambda msg, cfg: sent.append(msg))
    msg = sent[0]
    assert msg["Subject"] == "[dqwatch] 2 check(s) failing on orders"
    assert msg["To"] == "me@example.com"
    body = msg.get_content()
    assert "r1" in body and "3 empty emails" in body and "fetch" in body


def test_from_env_none_when_unset(monkeypatch):
    for k in ("DQ_SMTP_HOST", "DQ_ALERT_TO"):
        monkeypatch.delenv(k, raising=False)
    assert notify.smtp_from_env() is None
    monkeypatch.setenv("DQ_SMTP_HOST", "h")
    monkeypatch.setenv("DQ_ALERT_TO", "t@example.com")
    cfg = notify.smtp_from_env()
    assert cfg.host == "h" and cfg.port == 587 and cfg.to == "t@example.com"
