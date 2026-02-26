import argparse
import asyncio
import re
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

from telethon import functions
from telethon.tl import types

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
except Exception:
    pass

from src.telegram_notify import send_telegram_message  # noqa: E402
from src.telegram_telethon import load_telethon_auth, make_telethon_client  # noqa: E402


def clean_chat_ref(v: str) -> str:
    s = str(v or "").strip()
    if s.startswith("https://t.me/") or s.startswith("http://t.me/"):
        s = re.sub(r"^https?://t\.me/", "", s, flags=re.IGNORECASE)
        s = s.split("?", 1)[0].split("#", 1)[0].strip("/")
        s = s.split("/", 1)[0]
        if s:
            return "@" + s
    return s


def read_refs_from_file(path: Path) -> List[str]:
    if not path.exists():
        return []
    out: List[str] = []
    for line in path.read_text(encoding="utf-8-sig").splitlines():
        s = line.lstrip("\ufeff").strip()
        if s and not s.startswith("#"):
            out.append(s)
    return out


def split_refs(raw: str) -> List[str]:
    parts = re.split(r"[\r\n,;]+", str(raw or ""))
    return [x.strip() for x in parts if x.strip()]


def title_text(v: Any) -> str:
    if isinstance(v, str):
        return v
    t = getattr(v, "text", None)
    if isinstance(t, str):
        return t
    return str(v or "")


def make_title(value: str) -> Any:
    return types.TextWithEntities(text=str(value or "").strip(), entities=[])


def peer_key(p: Any) -> str:
    if isinstance(p, types.InputPeerChannel):
        return f"channel:{p.channel_id}:{p.access_hash}"
    if isinstance(p, types.InputPeerChat):
        return f"chat:{p.chat_id}"
    if isinstance(p, types.InputPeerUser):
        return f"user:{p.user_id}:{p.access_hash}"
    return repr(p)


def merge_unique_peers(existing: Sequence[Any], extra: Sequence[Any]) -> List[Any]:
    out: List[Any] = []
    seen: Set[str] = set()
    for p in list(existing) + list(extra):
        k = peer_key(p)
        if k in seen:
            continue
        seen.add(k)
        out.append(p)
    return out


async def run(args: argparse.Namespace) -> int:
    refs: List[str] = []
    refs.extend(split_refs(args.refs))
    file_path = Path(args.sources_file) if Path(args.sources_file).is_absolute() else (ROOT / args.sources_file)
    refs.extend(read_refs_from_file(file_path))
    refs = [clean_chat_ref(x) for x in refs if clean_chat_ref(x)]
    refs = list(dict.fromkeys(refs))
    if args.max_refs > 0:
        refs = refs[: args.max_refs]
    if not refs:
        print("[tg-folder] no refs found")
        return 2

    auth = load_telethon_auth(ROOT)
    client = make_telethon_client(auth)

    async with client:
        peer_inputs: List[Any] = []
        resolved = 0
        failed: List[Tuple[str, str]] = []
        for ref in refs:
            try:
                ent = await client.get_entity(ref)
                ip = await client.get_input_entity(ent)
                peer_inputs.append(ip)
                resolved += 1
            except Exception as e:
                failed.append((ref, str(e)))

        resp = await client(functions.messages.GetDialogFiltersRequest())
        filters = list(getattr(resp, "filters", []) or [])
        existing: Optional[types.DialogFilter] = None
        for f in filters:
            if isinstance(f, types.DialogFilter) and title_text(f.title).strip().lower() == args.folder_title.strip().lower():
                existing = f
                break

        if existing is not None:
            folder_id = int(existing.id)
            if args.strict_sync:
                merged = merge_unique_peers([], peer_inputs)
            else:
                merged = merge_unique_peers(existing.include_peers or [], peer_inputs)
            folder = types.DialogFilter(
                id=folder_id,
                title=existing.title if args.keep_existing_title else make_title(args.folder_title),
                pinned_peers=existing.pinned_peers or [],
                include_peers=merged,
                exclude_peers=existing.exclude_peers or [],
                contacts=existing.contacts,
                non_contacts=existing.non_contacts,
                groups=existing.groups,
                broadcasts=existing.broadcasts,
                bots=existing.bots,
                exclude_muted=existing.exclude_muted,
                exclude_read=existing.exclude_read,
                exclude_archived=existing.exclude_archived,
                title_noanimate=getattr(existing, "title_noanimate", None),
                emoticon=getattr(existing, "emoticon", None),
                color=getattr(existing, "color", None),
            )
            before_count = len(existing.include_peers or [])
            after_count = len(merged)
            mode = "updated"
        else:
            used_ids = {int(f.id) for f in filters if isinstance(f, types.DialogFilter)}
            folder_id = args.folder_id if args.folder_id > 0 else 2
            while folder_id in used_ids:
                folder_id += 1
            merged = merge_unique_peers([], peer_inputs)
            folder = types.DialogFilter(
                id=folder_id,
                title=make_title(args.folder_title),
                pinned_peers=[],
                include_peers=merged,
                exclude_peers=[],
                contacts=False,
                non_contacts=False,
                groups=False,
                broadcasts=False,
                bots=False,
                exclude_muted=False,
                exclude_read=False,
                exclude_archived=False,
                emoticon=args.folder_emoticon or None,
            )
            before_count = 0
            after_count = len(merged)
            mode = "created"

        if args.dry_run:
            print(f"[tg-folder] dry-run {mode} id={folder_id} title={args.folder_title!r} peers {before_count}->{after_count}")
        else:
            await client(functions.messages.UpdateDialogFilterRequest(id=folder_id, filter=folder))
            print(f"[tg-folder] {mode} id={folder_id} title={args.folder_title!r} peers {before_count}->{after_count}")

        print(f"[tg-folder] refs={len(refs)} resolved={resolved} failed={len(failed)}")
        for ref, why in failed[:15]:
            print(f"[tg-folder] resolve-fail {ref}: {why}")

        if args.telegram:
            send_telegram_message(
                "\n".join(
                    [
                        "AIJobSearcher: Telegram folder sync",
                        f"Folder: {args.folder_title}",
                        f"Mode: {mode}",
                        f"Peers: {before_count} -> {after_count}",
                        f"Refs: {len(refs)} resolved={resolved} failed={len(failed)}",
                    ]
                )
            )

    return 0


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Assign Telegram sources to a dialog folder via Telethon user session.")
    ap.add_argument("--folder-title", default="🖥AI Auto Gig")
    ap.add_argument("--folder-emoticon", default="💼")
    ap.add_argument("--folder-id", type=int, default=0, help="Optional explicit folder id. 0 = auto.")
    ap.add_argument("--keep-existing-title", action="store_true")
    ap.add_argument("--strict-sync", dest="strict_sync", action="store_true", help="Folder peers = exactly sources refs.")
    ap.add_argument("--no-strict-sync", dest="strict_sync", action="store_false", help="Merge with existing folder peers.")
    ap.set_defaults(strict_sync=True)
    ap.add_argument("--sources-file", default="data/inbox/telegram_chats.txt")
    ap.add_argument("--refs", default="", help="Optional refs list (@name,t.me/name) to merge with sources-file.")
    ap.add_argument("--max-refs", type=int, default=400)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--telegram", action="store_true")
    return ap


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    rc = asyncio.run(run(args))
    raise SystemExit(rc)


if __name__ == "__main__":
    main()
