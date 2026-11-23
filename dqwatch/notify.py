"""Email alerts over SMTP (e.g. Gmail with an app password)."""
import os
import smtplib
from dataclasses import dataclass
from email.message import EmailMessage

from dqwatch.models import CheckResult


@dataclass
class SmtpConfig:
    host: str
    port: int
    user: str
    password: str
    to: str


def smtp_from_env() -> SmtpConfig | None:
    host, to = os.environ.get("DQ_SMTP_HOST"), os.environ.get("DQ_ALERT_TO")
    if not host or not to:
        return None
    return SmtpConfig(host, int(os.environ.get("DQ_SMTP_PORT", "587")), os.environ.get("DQ_SMTP_USER", ""),
                      os.environ.get("DQ_SMTP_PASSWORD", ""), to)


def _smtp_send(msg: EmailMessage, cfg: SmtpConfig):
    with smtplib.SMTP(cfg.host, cfg.port, timeout=30) as s:
        s.starttls()
        if cfg.user:
            s.login(cfg.user, cfg.password)
        s.send_message(msg)


def send_alert(source_name: str, failures: list[CheckResult], cfg: SmtpConfig, send=None) -> None:
    msg = EmailMessage()
    msg["Subject"] = f"[dqwatch] {len(failures)} check(s) failing on {source_name}"
    msg["From"] = cfg.user or cfg.to
    msg["To"] = cfg.to
    lines = [f"These checks started failing on '{source_name}'. The data was still loaded.", ""]
    for f in failures:
        lines.append(f"- {f.rule_id}: {f.status} ({f.failing_count} row(s)) {f.detail}".rstrip())
    msg.set_content("\n".join(lines) + "\n")
    (send or _smtp_send)(msg, cfg)
