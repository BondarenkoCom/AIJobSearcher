import argparse
import csv
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def _norm(s: str) -> str:
    return (s or "").strip()


def _norm_email(s: str) -> str:
    return _norm(s).lower()


def _norm_company(s: str) -> str:
    # Keep it simple; we mostly need a stable "unique company" key.
    return " ".join(_norm(s).lower().split())


def _read_csv_rows(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.DictReader(f)
        return list(reader)


def _load_sent_emails(sent_log_path: Path) -> Set[str]:
    if not sent_log_path.exists():
        return set()
    rows = _read_csv_rows(sent_log_path)
    return {_norm_email(r.get("to_email", "")) for r in rows if _norm_email(r.get("to_email", ""))}


@dataclass
class Lead:
    file_source: str
    row_source: str
    contact_email: str
    title: str
    company: str
    location: str
    url: str
    marked_in_csv: bool
    sent_by_bot: bool


def _is_sent_row(row: Dict[str, str]) -> bool:
    if _norm(row.get("sent_at", "")):
        return True
    if _norm(row.get("sent_status", "")).lower() == "sent":
        return True
    return False


def _iter_leads(
    csv_paths: Iterable[Path],
    sent_emails: Set[str],
) -> List[Lead]:
    leads: List[Lead] = []
    for p in csv_paths:
        rows = _read_csv_rows(p)
        for r in rows:
            email = _norm_email(r.get("contact_email", ""))
            if not email or "@" not in email:
                continue
            file_source = p.name
            row_source = _norm(r.get("source", "")) or file_source
            marked = _is_sent_row(r)
            sent_by_bot = email in sent_emails

            leads.append(
                Lead(
                    file_source=file_source,
                    row_source=row_source,
                    contact_email=email,
                    title=_norm(r.get("title", "")),
                    company=_norm(r.get("company", "")),
                    location=_norm(r.get("location", "")),
                    url=_norm(r.get("url", "")),
                    marked_in_csv=marked,
                    sent_by_bot=sent_by_bot,
                )
            )
    return leads


def _unique_companies(leads: Iterable[Lead]) -> Set[str]:
    out: Set[str] = set()
    for l in leads:
        key = _norm_company(l.company)
        if key:
            out.add(key)
    return out


def _unique_emails(leads: Iterable[Lead]) -> Set[str]:
    return {l.contact_email for l in leads if l.contact_email}


def _write_inventory(out_path: Path, leads: List[Lead]) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "file_source",
                "row_source",
                "contact_email",
                "company",
                "title",
                "location",
                "url",
                "marked_in_csv",
                "sent_by_bot",
            ],
        )
        w.writeheader()
        for l in leads:
            w.writerow(
                {
                    "file_source": l.file_source,
                    "row_source": l.row_source,
                    "contact_email": l.contact_email,
                    "company": l.company,
                    "title": l.title,
                    "location": l.location,
                    "url": l.url,
                    "marked_in_csv": "1" if l.marked_in_csv else "0",
                    "sent_by_bot": "1" if l.sent_by_bot else "0",
                }
            )


def _fmt_pct(n: int, d: int) -> str:
    if d <= 0:
        return "0.0%"
    return f"{(n * 100.0 / d):.1f}%"


def main() -> int:
    ap = argparse.ArgumentParser(description="Lead/source stats for DatesFromAnothersSources")
    ap.add_argument("--leads-dir", default="DatesFromAnothersSources", help="Directory with lead CSV files")
    ap.add_argument("--sent-log", default="data/out/sent_log.csv", help="sent_log.csv path")
    ap.add_argument("--out-dir", default="data/out", help="Output directory")
    args = ap.parse_args()

    leads_dir = (ROOT / args.leads_dir).resolve()
    if not leads_dir.exists():
        print(f"[stats] missing dir: {leads_dir}")
        return 2

    csv_paths = sorted(leads_dir.glob("*.csv"))
    if not csv_paths:
        print(f"[stats] no csv files in: {leads_dir}")
        return 0

    sent_emails = _load_sent_emails((ROOT / args.sent_log).resolve())
    leads = _iter_leads(csv_paths, sent_emails)

    total_rows = len(leads)
    marked_rows = sum(1 for l in leads if l.marked_in_csv)
    bot_rows = sum(1 for l in leads if l.sent_by_bot)
    bot_pending_rows = total_rows - bot_rows

    marked_but_not_bot = sum(1 for l in leads if l.marked_in_csv and not l.sent_by_bot)
    bot_but_not_marked = sum(1 for l in leads if l.sent_by_bot and not l.marked_in_csv)

    uniq_emails = len(_unique_emails(leads))
    uniq_emails_bot = len(_unique_emails([l for l in leads if l.sent_by_bot]))

    uniq_companies = len(_unique_companies(leads))
    uniq_companies_bot = len(_unique_companies([l for l in leads if l.sent_by_bot]))

    by_file = defaultdict(lambda: Counter())
    by_row_source = defaultdict(lambda: Counter())
    for l in leads:
        by_file[l.file_source]["rows"] += 1
        by_file[l.file_source]["marked_in_csv"] += 1 if l.marked_in_csv else 0
        by_file[l.file_source]["sent_by_bot"] += 1 if l.sent_by_bot else 0

        by_row_source[l.row_source]["rows"] += 1
        by_row_source[l.row_source]["marked_in_csv"] += 1 if l.marked_in_csv else 0
        by_row_source[l.row_source]["sent_by_bot"] += 1 if l.sent_by_bot else 0

    out_dir = (ROOT / args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y-%m-%d")

    inv_path = out_dir / f"leads_inventory_{stamp}.csv"
    _write_inventory(inv_path, leads)

    # Pending (unique emails not yet sent by our bot)
    pending_map: Dict[str, Lead] = {}
    for l in leads:
        if l.sent_by_bot:
            continue
        # Keep the first-seen row as representative for this email.
        if l.contact_email not in pending_map:
            pending_map[l.contact_email] = l

    pending_path = out_dir / f"leads_pending_by_bot_{stamp}.csv"
    with pending_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "contact_email",
                "company",
                "title",
                "location",
                "file_source",
                "row_source",
                "marked_in_csv",
            ],
        )
        w.writeheader()
        for email in sorted(pending_map.keys()):
            l = pending_map[email]
            w.writerow(
                {
                    "contact_email": l.contact_email,
                    "company": l.company,
                    "title": l.title,
                    "location": l.location,
                    "file_source": l.file_source,
                    "row_source": l.row_source,
                    "marked_in_csv": "1" if l.marked_in_csv else "0",
                }
            )

    report_path = out_dir / f"leads_stats_{stamp}.txt"
    with report_path.open("w", encoding="utf-8") as f:
        f.write("Lead stats (DatesFromAnothersSources)\n")
        f.write(f"Generated: {datetime.now().isoformat(timespec='seconds')}\n\n")

        f.write(f"Rows (emails): {total_rows}\n")
        f.write(f"Marked as sent in CSV: {marked_rows} ({_fmt_pct(marked_rows, total_rows)})\n")
        f.write(f"Sent by our bot (sent_log): {bot_rows} ({_fmt_pct(bot_rows, total_rows)})\n")
        f.write(f"Not sent by bot: {bot_pending_rows} ({_fmt_pct(bot_pending_rows, total_rows)})\n\n")

        f.write(f"Marked in CSV, but NOT in sent_log: {marked_but_not_bot}\n")
        f.write(f"In sent_log, but NOT marked in CSV: {bot_but_not_marked}\n\n")

        f.write(f"Unique emails: {uniq_emails}\n")
        f.write(f"Unique emails sent by bot: {uniq_emails_bot} ({_fmt_pct(uniq_emails_bot, uniq_emails)})\n\n")

        f.write(f"Unique companies (by company field): {uniq_companies}\n")
        f.write(
            f"Unique companies with >=1 bot send: {uniq_companies_bot} ({_fmt_pct(uniq_companies_bot, uniq_companies)})\n\n"
        )

        f.write("By file (CSV):\n")
        for name in sorted(by_file.keys()):
            c = by_file[name]
            f.write(
                f"- {name}: rows={c['rows']} marked_in_csv={c['marked_in_csv']} bot_sent={c['sent_by_bot']}\n"
            )
        f.write("\n")

        f.write("By row source (source column):\n")
        for name in sorted(by_row_source.keys()):
            c = by_row_source[name]
            f.write(
                f"- {name}: rows={c['rows']} marked_in_csv={c['marked_in_csv']} bot_sent={c['sent_by_bot']}\n"
            )

        f.write("\n")
        f.write(f"Inventory CSV: {inv_path}\n")
        f.write(f"Pending-by-bot CSV: {pending_path}\n")

    print(f"[stats] wrote {report_path}")
    print(f"[stats] wrote {inv_path}")
    print(f"[stats] wrote {pending_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
