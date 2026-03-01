from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any, Dict

import yaml

from src.config import load_config
from src.offer_profiles import OfferProfile


def _merge(base: Any, override: Any) -> Any:
    if isinstance(base, dict) and isinstance(override, dict):
        merged = dict(base)
        for key, value in override.items():
            if key in merged:
                merged[key] = _merge(merged[key], value)
            else:
                merged[key] = deepcopy(value)
        return merged
    if isinstance(override, list):
        return deepcopy(override)
    return deepcopy(override)


def build_runtime_config(base_cfg_path: Path, offer: OfferProfile) -> Dict[str, Any]:
    base_cfg = load_config(str(base_cfg_path)) if base_cfg_path.exists() else {}
    overrides = dict(offer.config_overrides or {})
    if not overrides:
        return base_cfg
    return _merge(base_cfg, overrides)


def write_runtime_config(*, base_cfg_path: Path, offer: OfferProfile, out_path: Path) -> Path:
    cfg = build_runtime_config(base_cfg_path, offer)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(yaml.safe_dump(cfg, sort_keys=False, allow_unicode=False), encoding="utf-8")
    return out_path
