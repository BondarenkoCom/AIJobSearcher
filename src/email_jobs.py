import csv
import os
import random
import re
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

from .config import cfg_get, resolve_path
from .email_bounce import collect_bounced_addresses
from .email_sender import load_env_file, render_template, send_email_smtp
from .profile_store import normalize_person_name
from .activity_db import (  # noqa: E402
    LeadUpsert,
    add_event,
    add_to_blocklist,
    connect as db_connect,
    get_blocklist_contacts,
    get_event_counts_by_day,
    get_last_event_by_contact,
    init_db,
    upsert_lead,
    upsert_lead_with_flag,
)

SENT_LOG_HEADERS = [
    "timestamp",
    "to_email",
    "job_title",
    "company",
    "location",
    "source",
    "job_url",
]

QA_ROLE_RE = re.compile(r"\b(qa|quality assurance|sdet|test(?:er|ing)?|automation)\b", re.IGNORECASE)
NON_QA_HINT_RE = re.compile(
    r"\b(ux|ui\b|designer|design|product manager|marketing|sales|operations associate)\b",
    re.IGNORECASE,
)
TITLE_NOISE_RE = re.compile(
    r"\b(we(?:'|вЂ™)?re hiring|if you(?:'|вЂ™)?re|interested candidates|open positions?:|location:|experience:|members)\b",
    re.IGNORECASE,
)
TITLE_HARD_SKIP_RE = re.compile(
    r"\b(w2\s*only|only\s*w2|us citizen|green card|clearance required)\b",
    re.IGNORECASE,
)
BAD_EMAIL_DOM_SUFFIXES = (
    ".png",
    ".jpg",
    ".jpeg",
    ".gif",
    ".svg",
    ".webp",
    ".avif",
    ".heic",
    ".ico",
    ".css",
    ".js",
    ".woff",
    ".woff2",
)
BAD_EMAIL_LOCAL_HINT_RE = re.compile(
    r"(no-?reply|mailer-daemon|postmaster|abuse|privacy|legal|security|admin|support|sales|press|media|help|billing|feedback|accommodat|investor|partnership|webmaster|marketing|receipt|customer|success|service|advertis)",
    re.IGNORECASE,
)
BAD_EMAIL_DOMAIN_HINT_RE = re.compile(
    r"(example\.com|yourcompany\.com|domain\.com|company\.com|localhost)",
    re.IGNORECASE,
)


def _normalize_email(value: str) -> str:
    return value.strip().lower()


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _safe_int(value: object, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _safe_float(value: object, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _load_blacklist(path: Path) -> Set[str]:
    if not path.exists():
        return set()
    return {
        _normalize_email(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    }


def _append_blacklist(path: Path, emails: Iterable[str]) -> int:
    normalized = {_normalize_email(e) for e in emails if e}
    normalized.discard("")
    if not normalized:
        return 0

    existing = _load_blacklist(path) if path.exists() else set()
    to_add = sorted(e for e in normalized if e not in existing)
    if not to_add:
        return 0

    _ensure_parent(path)
    with path.open("a", encoding="utf-8") as f:
        for email in to_add:
            f.write(email + "\n")

    return len(to_add)


def _load_sent_log(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        return list(reader)


def _parse_timestamp(value: str) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _build_sent_maps(
    entries: Iterable[Dict[str, str]],
) -> Tuple[Dict[datetime.date, int], Dict[str, datetime]]:
    counts_by_date: Dict[datetime.date, int] = {}
    last_by_email: Dict[str, datetime] = {}

    for row in entries:
        ts = _parse_timestamp(row.get("timestamp", ""))
        if not ts:
            continue
        day = ts.date()
        counts_by_date[day] = counts_by_date.get(day, 0) + 1

        email = _normalize_email(row.get("to_email", ""))
        if not email:
            continue
        if email not in last_by_email or ts > last_by_email[email]:
            last_by_email[email] = ts

    return counts_by_date, last_by_email


def _calc_daily_cap(
    counts_by_date: Dict[datetime.date, int],
    daily_limit: int,
    max_increase: int,
    today: datetime.date,
) -> int:
    if daily_limit <= 0:
        daily_limit = 10**9
    if max_increase <= 0:
        max_increase = daily_limit

    previous_days = [day for day in counts_by_date if day < today]
    if not previous_days:
        return daily_limit

    last_day = max(previous_days)
    last_count = counts_by_date.get(last_day, 0)
    return min(daily_limit, last_count + max_increase)


def _append_sent_log(path: Path, row: Dict[str, str]) -> None:
    _ensure_parent(path)
    file_exists = path.exists()
    with path.open("a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=SENT_LOG_HEADERS)
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)


def _write_csv(path: Path, fieldnames: List[str], rows: List[Dict[str, str]]) -> None:
    _ensure_parent(path)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    tmp_path.replace(path)


def _acquire_send_lock(lock_path: Path, *, stale_seconds: int = 6 * 3600) -> bool:
    _ensure_parent(lock_path)
    now = time.time()
    if lock_path.exists():
        try:
            age = max(0.0, now - lock_path.stat().st_mtime)
            if age > max(60, stale_seconds):
                lock_path.unlink(missing_ok=True)
        except Exception:
            pass
    try:
        fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(f"pid={os.getpid()} started={datetime.now().isoformat(timespec='seconds')}\n")
        return True
    except FileExistsError:
        return False


def _is_vietnam_job(job: Dict[str, str]) -> bool:
    text = " ".join(
        [
            job.get("location", ""),
            job.get("title", ""),
            job.get("company", ""),
        ]
    ).lower()
    vn_keywords = [
        "vietnam",
        "viet nam",
        "ho chi minh",
        "hcmc",
        "hanoi",
        "da nang",
        "danang",
        "saigon",
        "hochiminh",
    ]
    return any(k in text for k in vn_keywords)


def _clean_job_title(value: str, default_title: str) -> str:
    title = (value or "").strip()
    if not title:
        return default_title
    title = title.replace("\u2019", "'")
    title = re.sub(r"\s+", " ", title).strip()
    title = re.sub(r"\s+View job(\s+View job)*\s*$", "", title, flags=re.IGNORECASE).strip()
    title = re.sub(r"\s+Job by\s+.+$", "", title, flags=re.IGNORECASE).strip()
    title = re.sub(
        r"\b(if you(?:'|вЂ™)?re|interested candidates|open positions?:|location:|experience:)\b.+$",
        "",
        title,
        flags=re.IGNORECASE,
    ).strip()
    title = re.sub(r"^we(?:'|вЂ™)?re hiring[:\s-]*", "", title, flags=re.IGNORECASE).strip()
    title = title.strip(" -|:,")
    if TITLE_NOISE_RE.search(title):
        title = ""
    if TITLE_HARD_SKIP_RE.search(title):
        title = ""
    if NON_QA_HINT_RE.search(title):
        title = ""
    if title and not QA_ROLE_RE.search(title):
        title = ""
    if len(title) > 90:
        title = ""
    if len(title) > 120:
        title = title[:120].rstrip()
    return title or default_title


def _is_qa_title(title: str) -> bool:
    t = (title or "").strip()
    if not t:
        return False
    if not QA_ROLE_RE.search(t):
        return False
    if NON_QA_HINT_RE.search(t):
        return False
    if TITLE_HARD_SKIP_RE.search(t):
        return False
    return True


def _is_valid_contact_email(value: str) -> bool:
    e = _normalize_email(value)
    if not e or "@" not in e:
        return False
    local, domain = e.split("@", 1)
    if not local or "." not in domain:
        return False
    if any(domain.endswith(sfx) for sfx in BAD_EMAIL_DOM_SUFFIXES):
        return False
    if BAD_EMAIL_DOMAIN_HINT_RE.search(domain):
        return False
    if ".." in e or "/@" in e:
        return False
    if BAD_EMAIL_LOCAL_HINT_RE.search(local):
        return False
    return True


def _get_csv_path(root: Path, cfg: Dict[str, object]) -> Path:
    source = str(cfg_get(cfg, "email.csv_source", "shortlist")).lower()
    if source == "shortlist":
        filename = cfg_get(cfg, "output.write_shortlist", "jobs_shortlist.csv")
        return resolve_path(root, cfg_get(cfg, "output.out_dir", "data/out")) / filename
    if source == "all":
        filename = cfg_get(cfg, "output.write_all", "jobs_all.csv")
        return resolve_path(root, cfg_get(cfg, "output.out_dir", "data/out")) / filename
    return resolve_path(root, source)


def _role_pitch_for_title(title: str) -> str:
    t = (title or "").lower()
    if any(k in t for k in ("playwright", "selenium", "cypress", "ui")):
        return "I can quickly stabilize flaky UI automation and improve regression confidence."
    if any(k in t for k in ("api", "backend", "rest", "graphql")):
        return "I can strengthen API/auth regression checks and release reliability."
    if any(k in t for k in ("mobile", "ios", "android", "appium")):
        return "I can improve mobile test coverage and reduce release risk."
    if any(k in t for k in ("performance", "load", "jmeter", "gatling")):
        return "I can run practical performance checks and triage bottlenecks fast."
    if any(k in t for k in ("sdet", "automation", "test automation")):
        return "I can deliver practical test automation improvements with CI-ready checks."
    return "I can help with practical QA automation and API-focused validation."


def _build_variables(job: Dict[str, str], cfg: Dict[str, object]) -> Dict[str, str]:
    contact_name = normalize_person_name(str(job.get("contact_name") or "")).strip()
    variables = {
        "job_title": job.get("title", ""),
        "company": job.get("company", ""),
        "location": job.get("location", ""),
        "contact_name_or_team": contact_name or "Hiring Team",
        "candidate_name": normalize_person_name(str(cfg_get(cfg, "candidate.name", ""))),
        "phone": str(cfg_get(cfg, "candidate.phone", "")),
        "email": str(cfg_get(cfg, "candidate.email", "")),
        "linkedin": str(cfg_get(cfg, "candidate.linkedin", "")),
        "base_location": str(cfg_get(cfg, "candidate.base_location", "Ho Chi Minh City, Vietnam")),
        "timezone": str(cfg_get(cfg, "candidate.timezone", "UTC+7")),
    }
    variables["role_pitch"] = _role_pitch_for_title(str(job.get("title", "")))

    work_pref_remote = str(
        cfg_get(
            cfg,
            "email.work_pref_remote",
            "I am seeking remote-only roles (based in {base_location}, {timezone}).",
        )
    )
    work_pref_vietnam = str(
        cfg_get(
            cfg,
            "email.work_pref_vietnam",
            "I am based in {base_location} and can work onsite/hybrid.",
        )
    )
    work_preference = work_pref_vietnam if _is_vietnam_job(job) else work_pref_remote
    variables["work_preference"] = render_template(work_preference, variables)

    return variables


def send_applications(root: Path, cfg: Dict[str, object]) -> int:
    if not cfg_get(cfg, "email.enabled", False):
        return 0

    load_env_file(root / ".env")
    load_env_file(root / ".env.accounts")

    lock_path = resolve_path(root, str(cfg_get(cfg, "email.lock_path", "data/out/email_send.lock")))
    lock_stale_seconds = _safe_int(cfg_get(cfg, "email.lock_stale_seconds", 21600), 21600)
    has_lock = _acquire_send_lock(lock_path, stale_seconds=lock_stale_seconds)
    if not has_lock:
        print(f"[email] sender lock exists: {lock_path} (another send is running)")
        return 0

    activity_enabled = bool(cfg_get(cfg, "activity.enabled", False))
    db_conn = None

    def _finish(value: int) -> int:
        try:
            if db_conn is not None:
                db_conn.close()
        except Exception:
            pass
        try:
            lock_path.unlink(missing_ok=True)
        except Exception:
            pass
        return value
    if activity_enabled:
        try:
            db_path = resolve_path(root, str(cfg_get(cfg, "activity.db_path", "data/out/activity.sqlite")))
            db_conn = db_connect(db_path)
            init_db(db_conn)
        except Exception:
            db_conn = None

    csv_path = _get_csv_path(root, cfg)
    if not csv_path.exists():
        print(f"[email] csv not found: {csv_path}")
        return _finish(0)

    template_path = resolve_path(root, str(cfg_get(cfg, "email.template", "templates/email_en.txt")))
    template = template_path.read_text(encoding="utf-8") if template_path.exists() else ""
    if template:
        lines = template.splitlines()
        if lines and lines[0].strip().lower().startswith("subject:"):
            template = "\n".join(lines[1:]).lstrip()

    subject_tpl = str(cfg_get(cfg, "email.subject", "Application for {job_title}"))

    from_email = str(cfg_get(cfg, "email.from_email", ""))
    reply_to = str(cfg_get(cfg, "email.reply_to", "")) or None

    host = str(cfg_get(cfg, "email.smtp.host", "smtp.gmail.com"))
    port = _safe_int(cfg_get(cfg, "email.smtp.port", 587), 587)
    use_tls = bool(cfg_get(cfg, "email.smtp.use_tls", True))
    username = str(cfg_get(cfg, "email.smtp.username", "")) or None
    password_env = str(cfg_get(cfg, "email.smtp.password_env", "SMTP_PASSWORD"))
    password = os.environ.get(password_env)

    dry_run = bool(cfg_get(cfg, "email.dry_run", True))

    if not from_email:
        print("[email] from_email is empty; aborting")
        return _finish(0)

    if not dry_run and username and not password:
        print(f"[email] missing SMTP password in env var {password_env}; aborting")
        return _finish(0)

    attachments_cfg = cfg_get(cfg, "email.attachments", []) or []
    attachments = [resolve_path(root, str(p)) for p in attachments_cfg]

    blacklist_path = resolve_path(root, str(cfg_get(cfg, "email.blacklist_path", "data/out/blacklist.txt")))
    sent_log_path = resolve_path(root, str(cfg_get(cfg, "email.sent_log_path", "data/out/sent_log.csv")))

    blacklist = _load_blacklist(blacklist_path)

    counts_by_date: Dict[datetime.date, int] = {}
    last_by_email: Dict[str, datetime] = {}
    if db_conn is not None:
        try:
            for email in blacklist:
                add_to_blocklist(db_conn, contact=email, reason="file:blacklist.txt")
            db_conn.commit()

            blacklist.update(get_blocklist_contacts(db_conn))

            counts_by_date = get_event_counts_by_day(db_conn, "email_sent")
            last_by_email = get_last_event_by_contact(db_conn, "email_sent")
        except Exception:
            counts_by_date = {}
            last_by_email = {}

    if not counts_by_date and not last_by_email:
        sent_entries = _load_sent_log(sent_log_path)
        counts_by_date, last_by_email = _build_sent_maps(sent_entries)

    daily_limit = _safe_int(cfg_get(cfg, "email.rate_limit.daily_limit", 30), 30)
    max_daily_increase = _safe_int(cfg_get(cfg, "email.rate_limit.max_daily_increase", 30), 30)
    run_limit = _safe_int(cfg_get(cfg, "email.rate_limit.run_limit", 50), 50)
    skip_sent_days = _safe_int(cfg_get(cfg, "email.rate_limit.skip_sent_days", 90), 90)
    min_delay = _safe_float(cfg_get(cfg, "email.rate_limit.min_delay_sec", 25), 25)
    max_delay = _safe_float(cfg_get(cfg, "email.rate_limit.max_delay_sec", 90), 90)
    long_break_every = _safe_int(cfg_get(cfg, "email.rate_limit.long_break_every", 7), 7)
    long_break_min = _safe_float(cfg_get(cfg, "email.rate_limit.long_break_min_sec", 120), 120)
    long_break_max = _safe_float(cfg_get(cfg, "email.rate_limit.long_break_max_sec", 300), 300)
    delay_in_dry_run = bool(cfg_get(cfg, "email.rate_limit.apply_in_dry_run", False))

    if max_delay < min_delay:
        min_delay, max_delay = max_delay, min_delay
    if long_break_max < long_break_min:
        long_break_min, long_break_max = long_break_max, long_break_min

    bounce_cfg = cfg_get(cfg, "email.bounce_check", {}) or {}
    if bounce_cfg.get("enabled", False):
        imap_host = str(bounce_cfg.get("imap_host", "imap.gmail.com"))
        imap_username = str(bounce_cfg.get("imap_username") or from_email)
        imap_password_env = str(bounce_cfg.get("imap_password_env", "IMAP_PASSWORD"))
        imap_password = os.environ.get(imap_password_env)
        search = str(bounce_cfg.get("search", 'SUBJECT "Undelivered"'))
        days = _safe_int(bounce_cfg.get("days", 14), 14)
        max_messages = _safe_int(bounce_cfg.get("max_messages", 200), 200)
        mark_seen = bool(bounce_cfg.get("mark_seen", False))
        mailbox = str(bounce_cfg.get("mailbox", "INBOX"))

        if not imap_password:
            print(f"[email] missing IMAP password in env var {imap_password_env}; skip bounce check")
        else:
            ignore = [from_email, reply_to or "", imap_username, str(cfg_get(cfg, "candidate.email", ""))]
            bounced = collect_bounced_addresses(
                host=imap_host,
                username=imap_username,
                password=imap_password,
                search=search,
                days=days,
                max_messages=max_messages,
                mark_seen=mark_seen,
                ignore=ignore,
                mailbox=mailbox,
            )
            added = _append_blacklist(blacklist_path, bounced)
            if added:
                print(f"[email] bounce blacklist +{added}")
            blacklist.update(bounced)
            if db_conn is not None:
                try:
                    for email in bounced:
                        add_to_blocklist(db_conn, contact=email, reason="bounce_check")
                    db_conn.commit()
                except Exception:
                    pass

    today = datetime.now().date()
    daily_cap = _calc_daily_cap(counts_by_date, daily_limit, max_daily_increase, today)
    already_today = counts_by_date.get(today, 0)
    remaining_today = max(0, daily_cap - already_today)
    if run_limit > 0:
        remaining_today = min(remaining_today, run_limit)

    if remaining_today <= 0:
        print("[email] daily cap reached; nothing to send")
        return _finish(0)

    mark_sent_in_source = bool(cfg_get(cfg, "email.mark_sent_in_source", False))
    sent_field = str(cfg_get(cfg, "email.sent_field", "sent_at"))
    sent_status_field = str(cfg_get(cfg, "email.sent_status_field", "sent_status"))
    sent_status_value = str(cfg_get(cfg, "email.sent_status_value", "sent"))

    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames or [])
        rows = list(reader)

    if mark_sent_in_source and not dry_run:
        if sent_field and sent_field not in fieldnames:
            fieldnames.append(sent_field)
        if sent_status_field and sent_status_field not in fieldnames:
            fieldnames.append(sent_status_field)

    sent = 0
    qa_only = bool(cfg_get(cfg, "email.qa_only", True))
    default_title = str(cfg_get(cfg, "email.default_job_title", "QA Automation Engineer"))
    updated_source = False
    seen_in_run: Set[str] = set()
    for row in rows:
        if sent >= remaining_today:
            break
        to_email = (row.get("contact_email") or "").strip()
        if not _is_valid_contact_email(to_email):
            continue
        to_norm = _normalize_email(to_email)
        lead_id = None

        if db_conn is not None:
            try:
                now0 = datetime.now().isoformat(timespec="seconds")
                lead_id, inserted = upsert_lead_with_flag(
                    db_conn,
                    LeadUpsert(
                        platform="email",
                        lead_type="job",
                        contact=to_norm,
                        url=row.get("url", ""),
                        company=row.get("company", ""),
                        job_title=row.get("title", ""),
                        location=row.get("location", ""),
                        source=row.get("source", "") or str(csv_path.name),
                        created_at=now0,
                        raw=row,
                    ),
                )
                if inserted:
                    add_event(
                        db_conn,
                        lead_id=lead_id,
                        event_type="collected",
                        status="ok",
                        occurred_at=now0,
                        details={"csv": str(csv_path)},
                    )
                    db_conn.commit()
            except Exception:
                lead_id = None

        if sent_status_field and row.get(sent_status_field, "").strip().lower() == sent_status_value.lower():
            continue
        if sent_field and row.get(sent_field, "").strip():
            continue

        raw_title = row.get("title", "")
        if qa_only and str(raw_title or "").strip() and not _is_qa_title(str(raw_title or "")):
            continue
        clean_title = _clean_job_title(str(raw_title or ""), default_title)
        if qa_only and not _is_qa_title(clean_title):
            continue
        row["title"] = clean_title

        if to_norm in seen_in_run:
            continue
        if to_norm in blacklist:
            continue

        if skip_sent_days > 0:
            last_sent = last_by_email.get(to_norm)
            if last_sent and datetime.now() - last_sent < timedelta(days=skip_sent_days):
                if mark_sent_in_source and not dry_run and sent_field:
                    row[sent_field] = last_sent.isoformat(timespec="seconds")
                    if sent_status_field:
                        row[sent_status_field] = sent_status_value
                    updated_source = True
                continue

        variables = _build_variables(row, cfg)
        subject = render_template(subject_tpl, variables)
        body = render_template(template, variables)
        ok = send_email_smtp(
            host=host,
            port=port,
            use_tls=use_tls,
            username=username,
            password=password,
            from_email=from_email,
            to_email=to_email,
            subject=subject,
            body=body,
            attachments=attachments,
            reply_to=reply_to,
            dry_run=dry_run,
        )
        if not ok:
            continue

        sent += 1
        seen_in_run.add(to_norm)

        if not dry_run:
            now = datetime.now()
            if db_conn is not None:
                try:
                    lid = lead_id or upsert_lead(
                        db_conn,
                        LeadUpsert(
                            platform="email",
                            lead_type="job",
                            contact=to_norm,
                            url=row.get("url", ""),
                            company=row.get("company", ""),
                            job_title=row.get("title", ""),
                            location=row.get("location", ""),
                            source=row.get("source", "") or str(csv_path.name),
                            created_at=now.isoformat(timespec="seconds"),
                            raw=row,
                        ),
                    )
                    add_event(
                        db_conn,
                        lead_id=lid,
                        event_type="email_sent",
                        status="ok",
                        occurred_at=now.isoformat(timespec="seconds"),
                        details={"csv": str(csv_path)},
                    )
                    db_conn.commit()
                except Exception:
                    pass
            _append_sent_log(
                sent_log_path,
                {
                    "timestamp": now.isoformat(timespec="seconds"),
                    "to_email": to_email,
                    "job_title": row.get("title", ""),
                    "company": row.get("company", ""),
                    "location": row.get("location", ""),
                    "source": row.get("source", ""),
                    "job_url": row.get("url", ""),
                },
            )
            counts_by_date[today] = counts_by_date.get(today, 0) + 1
            last_by_email[to_norm] = now

            if mark_sent_in_source and sent_field:
                row[sent_field] = now.isoformat(timespec="seconds")
                if sent_status_field:
                    row[sent_status_field] = sent_status_value
                updated_source = True

        apply_delays = (not dry_run) or delay_in_dry_run
        if apply_delays and sent < remaining_today:
            delay = random.uniform(min_delay, max_delay)
            print(f"[email] sleep {delay:.1f}s")
            time.sleep(delay)
            if long_break_every > 0 and sent % long_break_every == 0:
                long_delay = random.uniform(long_break_min, long_break_max)
                print(f"[email] long break {long_delay:.1f}s")
                time.sleep(long_delay)

    if mark_sent_in_source and updated_source and not dry_run:
        _write_csv(csv_path, fieldnames, rows)

    try:
        from .notify import notify_done

        notify_done(root, cfg)
    except Exception:
        pass

    return _finish(sent)
