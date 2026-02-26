import argparse
import asyncio
import re
import sys
from pathlib import Path
from typing import List, Sequence, Set

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
    "remote",
    "contract",
    "work",
    "qa",
    "tester",
    "test",
    "sdet",
    "automation",
    "РІР°РєР°РЅ",
    "СЂР°Р±РѕС‚Р°",
    "С„СЂРёР»Р°РЅСЃ",
    "СѓРґР°Р»РµРЅ",
    "СѓРґР°Р»С‘РЅ",
    "С‚РµСЃС‚РёСЂРѕРІ",
)
NOISE_TERMS = (
    "crypto",
    "pump",
    "casino",
    "bet",
    "forex",
    "signals",
    "airdrop",
    "binary",
)


def read_list_file(path: Path) -> List[str]:
    if not path.exists():
        return []
    out: List[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        s = line.strip().lstrip("\ufeff").strip()
        if s and not s.startswith("#"):
            out.append(s)
    return out


def append_unique_lines(path: Path, values: Sequence[str]) -> List[str]:
    path.parent.mkdir(parents=True, exist_ok=True)
    existing = {line.strip().lower() for line in read_list_file(path)}
    added: List[str] = []
    for v in values:
        s = str(v or "").strip()
        if not s:
            continue
        if not s.startswith("@"):
            s = "@" + s.lstrip("@")
        if s.lower() in existing:
            continue
        existing.add(s.lower())
        added.append(s)
    if added:
        prefix = "\n" if path.exists() and path.read_text(encoding="utf-8").strip() else ""
        with path.open("a", encoding="utf-8") as f:
            f.write(prefix)
            for x in added:
                f.write(x + "\n")
    return added


def _score_text(text: str) -> int:
    low = str(text or "").lower()
    pos = sum(1 for t in JOB_TERMS if t in low)
    neg = sum(2 for t in NOISE_TERMS if t in low)
    return pos - neg


def _clean_title(v: str) -> str:
    return re.sub(r"\s+", " ", str(v or "")).strip()


async def run(args: argparse.Namespace) -> int:
    src_file = Path(args.sources_file) if Path(args.sources_file).is_absolute() else (ROOT / args.sources_file)
    existing = {x.lower() for x in read_list_file(src_file)}

    auth = load_telethon_auth(ROOT)
    client = make_telethon_client(auth)

    candidates: List[str] = []
    scanned = 0
    async with client:
        async for dlg in client.iter_dialogs(limit=None):
            scanned += 1
            ent = dlg.entity
            uname = str(getattr(ent, "username", "") or "").strip()
            if not uname:
                continue
            ref = "@" + uname
            if ref.lower() in existing:
                continue

            title = _clean_title(str(getattr(ent, "title", "") or getattr(ent, "first_name", "") or uname))
            if not title:
                continue

            score = _score_text(title + " " + uname)

            if score < int(args.min_score):
                try:
                    txt = _clean_title(str(getattr(dlg, "message", None).message or ""))
                except Exception:
                    txt = ""
                if txt:
                    score = max(score, _score_text(txt))

            if score >= int(args.min_score):
                candidates.append(ref)
                if int(args.max_add) > 0 and len(candidates) >= int(args.max_add):
                    break

    added = append_unique_lines(src_file, candidates) if args.append else candidates
    print(f"[tg-import-old] scanned_dialogs={scanned}")
    print(f"[tg-import-old] matched_candidates={len(candidates)}")
    if args.append:
        print(f"[tg-import-old] appended_sources={len(added)} file={src_file}")
    else:
        print("[tg-import-old] dry-run (no append)")
    if added:
        for x in added[:40]:
            print(f"- {x}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Import already-joined Telegram job-like dialogs into sources file.")
    ap.add_argument("--sources-file", default="data/inbox/telegram_chats.txt")
    ap.add_argument("--append", action="store_true", help="Append matched refs to sources file")
    ap.add_argument("--max-add", type=int, default=40)
    ap.add_argument("--min-score", type=int, default=1)
    return ap


def main() -> int:
    return asyncio.run(run(build_parser().parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
