from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List

import yaml


@dataclass
class OfferProfile:
    slug: str
    title: str
    summary: str
    product_hint: str
    scanners: List[Dict[str, Any]]
    export: Dict[str, Any]
    bot: Dict[str, Any]
    config_overrides: Dict[str, Any]


def load_offer_profiles(path: Path) -> Dict[str, OfferProfile]:
    if not path.exists():
        raise FileNotFoundError(f"Offer config not found: {path}")
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    offers_raw = raw.get("offers") or {}
    out: Dict[str, OfferProfile] = {}
    for slug, item in offers_raw.items():
        if not isinstance(item, dict):
            continue
        out[slug] = OfferProfile(
            slug=str(slug).strip(),
            title=str(item.get("title") or slug).strip(),
            summary=str(item.get("summary") or "").strip(),
            product_hint=str(item.get("product_hint") or "").strip(),
            scanners=list(item.get("scanners") or []),
            export=dict(item.get("export") or {}),
            bot=dict(item.get("bot") or {}),
            config_overrides=dict(item.get("config_overrides") or {}),
        )
    return out
