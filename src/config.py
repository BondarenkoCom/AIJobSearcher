from pathlib import Path
from typing import Any, Dict
import yaml


def load_config(path: str) -> Dict[str, Any]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Config not found: {p}")
    data = yaml.safe_load(p.read_text(encoding="utf-8"))
    return data or {}


def cfg_get(cfg: Dict[str, Any], key_path: str, default: Any = None) -> Any:
    cur: Any = cfg
    for key in key_path.split("."):
        if not isinstance(cur, dict) or key not in cur:
            return default
        cur = cur[key]
    return cur


def resolve_path(base_dir: Path, value: str) -> Path:
    p = Path(value)
    return p if p.is_absolute() else base_dir / p
