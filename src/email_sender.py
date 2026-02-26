import os
import smtplib
from email.message import EmailMessage
from pathlib import Path
from typing import Dict, Iterable, Optional


class SafeDict(dict):
    def __missing__(self, key: str) -> str:
        return "{" + key + "}"


def load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8-sig").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'\"")
        if key and key not in os.environ:
            os.environ[key] = value


def render_template(template: str, variables: Dict[str, str]) -> str:
    return template.format_map(SafeDict(variables))


def send_email_smtp(
    *,
    host: str,
    port: int,
    use_tls: bool,
    username: Optional[str],
    password: Optional[str],
    from_email: str,
    to_email: str,
    subject: str,
    body: str,
    attachments: Iterable[Path],
    reply_to: Optional[str] = None,
    dry_run: bool = True,
) -> bool:
    if dry_run:
        print(f"[dry_run] to={to_email} subject={subject}")
        return True

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = from_email
    msg["To"] = to_email
    if reply_to:
        msg["Reply-To"] = reply_to
    msg.set_content(body)

    for path in attachments:
        if not path.exists():
            print(f"[email] attachment missing: {path}")
            continue
        data = path.read_bytes()
        msg.add_attachment(
            data,
            maintype="application",
            subtype="pdf",
            filename=path.name,
        )

    with smtplib.SMTP(host, port, timeout=30) as smtp:
        if use_tls:
            smtp.starttls()
        if username and password:
            smtp.login(username, password)
        smtp.send_message(msg)

    return True
