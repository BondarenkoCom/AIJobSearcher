import json
from typing import Any, Dict, Iterable, List, Optional

import requests

from ..models import Job


def _get_by_path(obj: Any, path: Optional[Any]) -> Any:
    if not path:
        return None
    if isinstance(path, (list, tuple)):
        parts = [str(p) for p in path]
    else:
        parts = [p for p in str(path).split(".") if p]

    cur = obj
    for part in parts:
        if isinstance(cur, dict):
            cur = cur.get(part)
        elif isinstance(cur, list):
            try:
                idx = int(part)
            except ValueError:
                return None
            if idx < 0 or idx >= len(cur):
                return None
            cur = cur[idx]
        else:
            return None
    return cur


def _extract_list(data: Any, list_path: Optional[Any]) -> List[Any]:
    if list_path:
        items = _get_by_path(data, list_path)
        return items if isinstance(items, list) else []
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("results", "jobs", "data", "items"):
            items = data.get(key)
            if isinstance(items, list):
                return items
    return []


def _to_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return str(value)


def _map_job(item: Any, mapping: Dict[str, Any], static: Dict[str, Any], source_name: str) -> Job:
    def get_field(name: str) -> str:
        val = _get_by_path(item, mapping.get(name)) if mapping else None
        if (val is None or val == "") and name in static:
            val = static[name]
        return _to_text(val)

    title = get_field("title")
    company = get_field("company")
    location = get_field("location")
    url = get_field("url")
    description = get_field("description")
    contact_email = get_field("contact_email")
    source = get_field("source") or source_name

    return Job(
        title=title,
        company=company,
        location=location,
        url=url,
        description=description,
        contact_email=contact_email,
        source=source,
        raw=item if isinstance(item, dict) else {"item": item},
    )


def collect_from_http_json(cfg: Dict[str, Any]) -> List[Job]:
    name = str(cfg.get("name") or "http_json")
    url = cfg.get("url")
    if not url:
        return []

    method = str(cfg.get("method") or "GET").upper()
    params = cfg.get("params") or {}
    headers = cfg.get("headers") or {}
    json_body = cfg.get("json") or None
    timeout = float(cfg.get("timeout_sec") or 20)

    try:
        if method == "GET":
            resp = requests.request(method, url, params=params, headers=headers, timeout=timeout)
        else:
            resp = requests.request(method, url, params=params, headers=headers, json=json_body, timeout=timeout)
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        print(f"[http_json:{name}] failed: {exc}")
        return []

    items = _extract_list(data, cfg.get("list_path"))
    mapping = cfg.get("mapping") or {}
    static = cfg.get("static") or {}

    jobs: List[Job] = []
    for item in items:
        job = _map_job(item, mapping, static, name)
        if not job.title or not job.company:
            continue
        jobs.append(job)

    return jobs
