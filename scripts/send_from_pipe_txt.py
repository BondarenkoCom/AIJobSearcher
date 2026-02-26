import argparse
import csv
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.config import load_config, resolve_path  # noqa: E402
from src.email_jobs import send_applications  # noqa: E402


EMAIL_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.IGNORECASE)
QA_ROLE_RE = re.compile(r"\b(qa|quality assurance|sdet|test(ing)?|automation)\b", re.IGNORECASE)
NON_QA_HINT_RE = re.compile(
    r"\b(ux|ui\b|designer|design|product manager|privacy steward|operations associate|marketing|sales)\b",
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


def parse_pipe_line(line: str) -> Tuple[str, str, str, str]:
    """
    Expected format (from other agents):
      email | job title | location | company
    Extra pipes are joined into the last field.
    """
    parts = [p.strip() for p in line.split("|")]
    parts = [p for p in parts if p != ""]
    if not parts:
        return "", "", "", ""

    email_idx = 0
    for idx, part in enumerate(parts):
        if "@" in part:
            email_idx = idx
            break

    email = parts[email_idx]
    rest = parts[email_idx + 1 :]
    if len(rest) >= 3:
        company = rest[-1]
        location = rest[-2]
        title = " | ".join(rest[:-2]).strip()
    elif len(rest) == 2:
        title, location = rest
        company = ""
    elif len(rest) == 1:
        title = rest[0]
        location = ""
        company = ""
    else:
        title = ""
        location = ""
        company = ""
    return email, title, location, company


def to_job_row(email: str, title: str, location: str, company: str, source: str) -> Dict[str, str]:
    return {
        "title": title.strip(),
        "company": company.strip(),
        "location": location.strip(),
        "url": "",
        "description": "",
        "contact_email": email.strip(),
        "contact_name": "",
        "source": source,
    }


def clean_job_title(raw_title: str, default_title: str) -> str:
    t = (raw_title or "").strip()
    if not t:
        return default_title
    t = t.replace("\u2019", "'")
    t = re.sub(r"\s+", " ", t).strip()
    t = re.sub(r"\s+View job(\s+View job)*\s*$", "", t, flags=re.IGNORECASE).strip()
    t = re.sub(r"\s+Job by\s+.+$", "", t, flags=re.IGNORECASE).strip()
    t = re.sub(
        r"\b(if you(?:'|вЂ™)?re|interested candidates|open positions?:|location:|experience:)\b.+$",
        "",
        t,
        flags=re.IGNORECASE,
    ).strip()
    t = re.sub(r"^we(?:'|вЂ™)?re hiring[:\s-]*", "", t, flags=re.IGNORECASE).strip()
    t = t.strip(" -|:,")
    if TITLE_NOISE_RE.search(t):
        t = ""
    if TITLE_HARD_SKIP_RE.search(t):
        t = ""
    if NON_QA_HINT_RE.search(t):
        t = ""
    if t and not QA_ROLE_RE.search(t):
        t = ""
    if len(t) > 90:
        t = ""
    if len(t) > 120:
        t = t[:120].rstrip()
    return t or default_title


def is_qa_title(title: str) -> bool:
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


def is_valid_contact_email(value: str) -> bool:
    e = (value or "").strip().lower()
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


def write_csv(path: Path, rows: List[Dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "title",
        "company",
        "location",
        "url",
        "description",
        "contact_email",
        "contact_name",
        "source",
        "sent_at",
        "sent_status",
    ]
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)


def main() -> int:
    ap = argparse.ArgumentParser(description="Convert 'email | title | location | company' TXT to CSV and send emails.")
    ap.add_argument("--config", default="config/config.yaml", help="Path to config YAML")
    ap.add_argument("--txt", required=True, help="Input TXT path (pipe-separated)")
    ap.add_argument("--out-csv", default="", help="Output CSV path (default: same folder, dated name)")
    ap.add_argument("--dry-run", action="store_true", help="Force dry-run (no emails)")
    ap.add_argument("--allow-non-qa", action="store_true", help="Do not filter non-QA roles")
    ap.add_argument("--default-title", default="QA Automation Engineer", help="Fallback title when source title is noisy/empty")
    args = ap.parse_args()

    txt_path = resolve_path(ROOT, args.txt)
    if not txt_path.exists():
        print(f"[pipe] missing: {txt_path}")
        return 2

    source_tag = txt_path.name

    raw = txt_path.read_text(encoding="utf-8", errors="replace").splitlines()
    rows: List[Dict[str, str]] = []
    invalid = 0
    skipped_non_qa = 0
    for line in raw:
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        email, title, location, company = parse_pipe_line(line)
        if not is_valid_contact_email(email):
            invalid += 1
            continue
        m = EMAIL_RE.search(email)
        if m:
            email = m.group(0)
        raw_title = title
        if not args.allow_non_qa and raw_title.strip() and not is_qa_title(raw_title):
            skipped_non_qa += 1
            continue
        clean_title = clean_job_title(raw_title, args.default_title)
        if not args.allow_non_qa and not is_qa_title(clean_title):
            skipped_non_qa += 1
            continue
        if not location.strip():
            location = "Remote"
        if not company.strip():
            company = "LinkedIn Lead"
        rows.append(to_job_row(email=email, title=clean_title, location=location, company=company, source=source_tag))

    if not rows:
        print(f"[pipe] no rows parsed (invalid={invalid}, non_qa={skipped_non_qa})")
        return 0

    stamp = datetime.now().strftime("%Y-%m-%d")
    out_csv = args.out_csv.strip()
    if not out_csv:
        out_csv_path = txt_path.with_name(f"{txt_path.stem}_{stamp}.csv")
    else:
        out_csv_path = resolve_path(ROOT, out_csv)

    write_csv(out_csv_path, rows)
    print(f"[pipe] parsed={len(rows)} invalid={invalid} non_qa_skipped={skipped_non_qa} -> {out_csv_path}")

    cfg_path = resolve_path(ROOT, args.config)
    cfg = load_config(str(cfg_path))

    cfg.setdefault("email", {})
    cfg["email"]["csv_source"] = str(out_csv_path)
    if args.dry_run:
        cfg["email"]["dry_run"] = True

    sent = send_applications(ROOT, cfg)
    print(f"[pipe] sent={sent}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
