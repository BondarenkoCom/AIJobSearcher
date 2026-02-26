import argparse
import asyncio
import csv
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Sequence, Tuple

from telethon import errors, types

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.telegram_telethon import load_telethon_auth, make_telethon_client  # noqa: E402


JOB_TERMS = (
    "job",
    "jobs",
    "hiring",
    "hire",
    "vacancy",
    "vacancies",
    "freelance",
    "gig",
    "contract",
    "remote",
    "qa",
    "tester",
    "testing",
    "test automation",
    "sdet",
    "automation",
    "вакан",
    "работа",
    "фриланс",
    "удален",
    "удалён",
    "тестиров",
)
BAD_TERMS = (
    "crypto",
    "casino",
    "bet",
    "airdrop",
    "forex",
    "signal",
    "mlm",
    "pump",
)


def read_refs(path: Path) -> List[str]:
    if not path.exists():
        return []
    out: List[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        s = line.strip().lstrip("\ufeff").strip()
        if s and not s.startswith("#"):
            out.append(s)
    # de-dup keep order
    seen = set()
    uniq: List[str] = []
    for x in out:
        k = x.lower()
        if k in seen:
            continue
        seen.add(k)
        uniq.append(x)
    return uniq


def score_text(text: str) -> int:
    low = str(text or "").lower()
    pos = sum(1 for t in JOB_TERMS if t in low)
    neg = sum(2 for t in BAD_TERMS if t in low)
    return pos - neg


def should_skip_username(uname: str) -> bool:
    u = str(uname or "").lower().strip()
    if not u:
        return False
    if u.endswith("bot"):
        return True
    if "bot" in u and ("job" not in u and "work" not in u):
        return True
    return False


def write_refs(path: Path, refs: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    header = [
        "# Fill this file with Telegram chat/channel refs (one per line), then run:",
        "#   .\\.venv\\Scripts\\python.exe scripts\\telegram_scan_gigs.py --write-db --telegram",
        "#",
        "# Accepted formats:",
        "# @channel_name",
        "# https://t.me/channel_name",
        "",
        "",
    ]
    txt = "\n".join(header + [str(x).strip() for x in refs if str(x).strip()]) + "\n"
    path.write_text(txt, encoding="utf-8")


async def run(args: argparse.Namespace) -> int:
    src = Path(args.sources_file) if Path(args.sources_file).is_absolute() else (ROOT / args.sources_file)
    refs = read_refs(src)
    if not refs:
        print("[tg-clean] no refs in sources file.")
        return 2

    auth = load_telethon_auth(ROOT)
    client = make_telethon_client(auth)

    kept: List[str] = []
    report: List[Dict[str, str]] = []
    scanned = 0
    async with client:
        for ref in refs:
            scanned += 1
            reason = ""
            keep = False
            title = ""
            uname = ref.lstrip("@")
            title_score = 0
            msg_score = 0

            try:
                ent = await client.get_entity(ref)
            except Exception as e:
                reason = f"resolve_failed:{type(e).__name__}"
                report.append({"ref": ref, "username": uname, "title": title, "keep": "0", "reason": reason})
                continue

            if isinstance(ent, types.User):
                reason = "is_user_not_channel"
                report.append({"ref": ref, "username": uname, "title": title, "keep": "0", "reason": reason})
                continue

            uname = str(getattr(ent, "username", "") or uname).strip()
            title = str(getattr(ent, "title", "") or ref).strip()
            if should_skip_username(uname):
                reason = "username_bot_like"
                report.append({"ref": ref, "username": uname, "title": title, "keep": "0", "reason": reason})
                continue

            title_score = score_text(f"{title} {uname}")
            if title_score >= int(args.title_min_score):
                keep = True
                reason = f"title_score={title_score}"
            else:
                # Fallback: inspect recent messages for job signals.
                max_s = -999
                try:
                    async for msg in client.iter_messages(ent, limit=max(1, int(args.msg_limit))):
                        txt = str(getattr(msg, "message", "") or "").strip()
                        if not txt:
                            continue
                        s = score_text(txt)
                        if s > max_s:
                            max_s = s
                    msg_score = max_s if max_s != -999 else 0
                except errors.RPCError:
                    msg_score = 0
                except Exception:
                    msg_score = 0
                if msg_score >= int(args.msg_min_score):
                    keep = True
                    reason = f"msg_score={msg_score}"
                else:
                    reason = f"low_signal:title={title_score},msg={msg_score}"

            if keep:
                out_ref = f"@{uname}" if uname else ref
                kept.append(out_ref)
            report.append(
                {
                    "ref": ref,
                    "username": uname,
                    "title": title,
                    "keep": "1" if keep else "0",
                    "reason": reason,
                }
            )

    # de-dup kept in order
    seen = set()
    uniq_kept: List[str] = []
    for x in kept:
        k = x.lower()
        if k in seen:
            continue
        seen.add(k)
        uniq_kept.append(x)

    backup = src.with_suffix(src.suffix + f".bak.{datetime.now().strftime('%Y%m%d_%H%M%S')}")
    if args.apply:
        backup.write_text(src.read_text(encoding="utf-8"), encoding="utf-8")
        write_refs(src, uniq_kept)
        print(f"[tg-clean] applied: kept={len(uniq_kept)} dropped={len(refs)-len(uniq_kept)} backup={backup}")
    else:
        print(f"[tg-clean] dry-run: kept={len(uniq_kept)} dropped={len(refs)-len(uniq_kept)}")

    out_csv = Path(args.out_csv) if Path(args.out_csv).is_absolute() else (ROOT / args.out_csv)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["ref", "username", "title", "keep", "reason"])
        w.writeheader()
        w.writerows(report)
    print(f"[tg-clean] scanned={scanned} csv={out_csv}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Clean Telegram sources file to keep only job-like channels/groups.")
    ap.add_argument("--sources-file", default="data/inbox/telegram_chats.txt")
    ap.add_argument("--title-min-score", type=int, default=1)
    ap.add_argument("--msg-min-score", type=int, default=2)
    ap.add_argument("--msg-limit", type=int, default=20)
    ap.add_argument("--apply", action="store_true", help="Rewrite sources file with kept refs")
    ap.add_argument("--out-csv", default="data/out/telegram_sources_clean_report.csv")
    return ap


def main() -> int:
    return asyncio.run(run(build_parser().parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
