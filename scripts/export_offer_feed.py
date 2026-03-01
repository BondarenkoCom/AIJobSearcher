from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Sequence

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.config import cfg_get, load_config, resolve_path  # noqa: E402
from src.email_sender import load_env_file  # noqa: E402
from src.offer_feed import build_offer_rows_from_db, load_offer, safe_text  # noqa: E402


def _write_json(path: Path, rows: Sequence[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(list(rows), ensure_ascii=False, indent=2), encoding="utf-8")


def _write_md(path: Path, offer: OfferProfile, rows: Sequence[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines: List[str] = [
        f"# {offer.title}",
        "",
        offer.summary,
        "",
        f"Updated: {datetime.now().isoformat(timespec='seconds')}",
        "",
    ]
    for idx, row in enumerate(rows, start=1):
        lines.append(f"## {idx}. {row.get('title') or 'Untitled'}")
        lines.append(f"- Company: {row.get('company') or '-'}")
        lines.append(f"- Platform: {row.get('platform') or '-'}")
        lines.append(f"- Contact: {row.get('contact_method') or '-'}")
        lines.append(f"- URL: {row.get('url') or '-'}")
        lines.append(f"- Snippet: {row.get('snippet') or '-'}")
        lines.append("")
    path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="Export a sellable lead feed for a chosen offer profile.")
    ap.add_argument("--config", default="config/config.yaml")
    ap.add_argument("--offers", default="config/offers.yaml")
    ap.add_argument("--offer", required=True)
    ap.add_argument("--db", default="")
    ap.add_argument("--scan-limit", type=int, default=600)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--out-dir", default="data/out")
    args = ap.parse_args()

    load_env_file(ROOT / ".env.accounts")
    load_env_file(ROOT / ".env")

    offer_path = resolve_path(ROOT, args.offers)
    try:
        offer = load_offer(offer_path, str(args.offer).strip())
    except KeyError:
        from src.offer_profiles import load_offer_profiles  # noqa: E402

        offers = load_offer_profiles(offer_path)
        print(f"[offer-export] unknown offer: {args.offer}")
        print(f"[offer-export] available: {', '.join(sorted(offers))}")
        return 2

    cfg_path = resolve_path(ROOT, args.config)
    cfg = load_config(str(cfg_path)) if cfg_path.exists() else {}
    db_path = resolve_path(ROOT, str(cfg_get(cfg, "activity.db_path", "data/out/activity.sqlite")))
    if safe_text(args.db):
        db_path = resolve_path(ROOT, safe_text(args.db))

    limit = int(args.limit) if int(args.limit) > 0 else int(offer.export.get("default_limit") or 10)
    selected = build_offer_rows_from_db(
        db_path=db_path,
        offers_path=offer_path,
        offer_slug=offer.slug,
        scan_limit=max(int(args.scan_limit), limit * 4),
        limit=limit,
    )

    out_dir = resolve_path(ROOT, args.out_dir)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = out_dir / f"offer_feed_{offer.slug}_{stamp}.json"
    md_path = out_dir / f"offer_feed_{offer.slug}_{stamp}.md"
    _write_json(json_path, selected)
    _write_md(md_path, offer, selected)

    print(f"[offer-export] offer={offer.slug} rows={len(selected)}")
    print(f"[offer-export] json={json_path}")
    print(f"[offer-export] md={md_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
