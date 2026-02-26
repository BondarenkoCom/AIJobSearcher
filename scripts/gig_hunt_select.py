import argparse
import csv
import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import requests
from urllib.parse import urlparse, urlunparse

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
except Exception:
    pass

from src.activity_db import connect as db_connect, init_db  # noqa: E402
from src.config import cfg_get, load_config, resolve_path  # noqa: E402
from src.email_sender import load_env_file  # noqa: E402
from src.telegram_notify import send_telegram_message  # noqa: E402


CONTACT_EVENTS = (
    "email_sent",
)

QA_RE = re.compile(
    r"\b(qa|quality\s+assurance|sdet|test\s*automation|automation\s*testing|tester|test\s*engineer|quality\s*engineer)\b",
    re.IGNORECASE,
)
AUTOMATION_RE = re.compile(
    r"\b(automation|playwright|selenium|cypress|appium|api\s*testing|rest|graphql|postman|c#|\.net|nunit|restsharp|python|javascript|ci\/cd|jenkins|gitlab)\b",
    re.IGNORECASE,
)
TECH_CONTEXT_RE = re.compile(
    r"\b(software|application|app|web|mobile|backend|frontend|api|platform|saas|testing|automation|qa|devops|engineer|code|script)\b",
    re.IGNORECASE,
)
SHORT_GIG_RE = re.compile(
    r"\b(quick|urgent|one[-\s]?time|small task|fix(?:ing)?\s+bug|few hours|24 ?h|48 ?h|1[-\s]?2 days?|2 days?)\b",
    re.IGNORECASE,
)
ONEOFF_HINT_RE = re.compile(
    r"\b(hourly|per\s*hour|\$+\s*\d+\s*/\s*h|fixed[-\s]?price|per\s*task|one[-\s]?off|freelance|contractor|paid)\b",
    re.IGNORECASE,
)
LONG_TERM_RE = re.compile(
    r"\b(full[-\s]?time|long[-\s]?term|ongoing|retainer|12\s*months?|6\s*months?|permanent)\b",
    re.IGNORECASE,
)
REMOTE_RE = re.compile(r"\b(remote|worldwide|global|anywhere|apac|asia)\b", re.IGNORECASE)
ONSITE_RE = re.compile(r"\b(on[-\s]?site|onsite|office|relocate)\b", re.IGNORECASE)
NONTECH_RE = re.compile(
    r"\b(potato|agri|farm|bakery|restaurant|cafe|barber|salon|cleaning|logistics driver|warehouse picker)\b",
    re.IGNORECASE,
)
NONFIT_TITLE_RE = re.compile(
    r"\b("
    r"cto|chief technology officer|full[-\s]?stack|web developer|frontend|backend developer|"
    r"mobile developer|salesforce|sap|consultant|project manager|data engineer|mulesoft|"
    r"java developer|python developer|cloud engineer|social media"
    r")\b",
    re.IGNORECASE,
)
AUTOMATION_GIG_RE = re.compile(
    r"\b(n8n|zapier|workflow|automation|bot|script|api integration|ci\/cd|test automation)\b",
    re.IGNORECASE,
)
OWN_CHAT_HINTS = ("my ai", "ai auto gig")


@dataclass
class Candidate:
    lead_id: str
    platform: str
    lead_type: str
    company: str
    title: str
    location: str
    url: str
    contact: str
    created_at: str
    remote_mode: str
    engagement: str
    budget: str
    emails: List[str]
    text: str
    source: str


def _now_stamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _safe(v: Any) -> str:
    return str(v or "").strip()


def _parse_json(raw_json: str) -> Dict[str, Any]:
    try:
        out = json.loads(raw_json or "{}")
        return out if isinstance(out, dict) else {}
    except Exception:
        return {}


def _extract_emails(raw: Dict[str, Any], contact: str) -> List[str]:
    out: List[str] = []
    raw_emails = raw.get("emails")
    if isinstance(raw_emails, list):
        for it in raw_emails:
            s = _safe(it).lower()
            if s and "@" in s:
                out.append(s)
    elif isinstance(raw_emails, str):
        for it in re.split(r"[;,\s]+", raw_emails):
            s = _safe(it).lower()
            if s and "@" in s:
                out.append(s)

    c = _safe(contact).lower()
    if "@" in c:
        out.append(c)

    uniq: List[str] = []
    seen = set()
    for e in out:
        if e in seen:
            continue
        seen.add(e)
        uniq.append(e)
    return uniq


def _compose_text(row: Dict[str, Any], raw: Dict[str, Any]) -> str:
    parts: List[str] = [
        _safe(row.get("job_title")),
        _safe(row.get("company")),
        _safe(row.get("location")),
        _safe(raw.get("text")),
        _safe(raw.get("description")),
        _safe(raw.get("snippet")),
        _safe(raw.get("one_liner")),
        _safe(raw.get("skills")),
    ]
    return "\n".join([p for p in parts if p]).strip()


def _canonical_url(url: str) -> str:
    u = _safe(url)
    if not u:
        return ""
    try:
        p = urlparse(u)
        netloc = (p.netloc or "").lower()
        path = (p.path or "").rstrip("/")
        if "linkedin.com" in netloc:
            if path.startswith("/jobs/view/") or path.startswith("/feed/update/") or path.startswith("/posts/"):
                return urlunparse((p.scheme or "https", p.netloc, path, "", "", ""))
        return urlunparse((p.scheme or "https", p.netloc, path, "", p.query, ""))
    except Exception:
        return u


def _extract_tg_handle(contact: str, url: str) -> str:
    c = _safe(contact)
    if c.lower().startswith("tg_username:"):
        core = c.split(":", 1)[1].strip().lstrip("@")
        return f"@{core}" if core else ""
    if c.startswith("@"):
        core = c.lstrip("@").strip()
        return f"@{core}" if core else ""
    u = _safe(url)
    m = re.search(r"https?://t\.me/([A-Za-z0-9_]{4,})", u, re.IGNORECASE)
    if m:
        return f"@{m.group(1)}"
    return ""


def _is_already_contacted(conn, lead_id: str, emails: Sequence[str]) -> bool:
    row = conn.execute(
        f"""
        SELECT 1
        FROM events
        WHERE lead_id = ?
          AND event_type IN ({",".join(["?"] * len(CONTACT_EVENTS))})
        LIMIT 1
        """,
        (lead_id, *CONTACT_EVENTS),
    ).fetchone()
    if row is not None:
        return True

    if emails:
        ph = ",".join(["?"] * len(emails))
        row = conn.execute(
            f"""
            SELECT 1
            FROM events e
            JOIN leads l ON l.lead_id = e.lead_id
            WHERE lower(l.contact) IN ({ph})
              AND e.event_type IN ({",".join(["?"] * len(CONTACT_EVENTS))})
            LIMIT 1
            """,
            (*[e.lower() for e in emails], *CONTACT_EVENTS),
        ).fetchone()
        if row is not None:
            return True

    return False


def _fetch_candidates(
    conn,
    *,
    platforms: Sequence[str],
    lead_types: Sequence[str],
    candidate_limit: int,
) -> List[Candidate]:
    if (not platforms) or (not lead_types):
        return []

    ph = ",".join(["?"] * len(platforms))
    lph = ",".join(["?"] * len(lead_types))
    rows = conn.execute(
        f"""
        SELECT lead_id, platform, lead_type, contact, url, company, job_title, location, source, raw_json, created_at
        FROM leads
        WHERE platform IN ({ph})
          AND lead_type IN ({lph})
        ORDER BY created_at DESC
        LIMIT ?
        """,
        (*platforms, *lead_types, int(candidate_limit) * 6),
    ).fetchall()

    out: List[Candidate] = []
    seen_url = set()
    for r in rows:
        row = {k: r[k] for k in r.keys()}
        raw = _parse_json(_safe(row.get("raw_json")))

        url = _safe(row.get("url")) or _safe(row.get("contact"))
        canon_url = _canonical_url(url)
        if not url:
            continue
        if canon_url and canon_url in seen_url:
            continue

        title = _safe(row.get("job_title"))
        text = _compose_text(row, raw)
        if not text:
            continue

        if _safe(row.get("platform")).lower() == "telegram":
            merged_meta = " ".join(
                [
                    _safe(row.get("company")).lower(),
                    _safe(row.get("job_title")).lower(),
                    _safe(row.get("source")).lower(),
                ]
            )
            if any(h in merged_meta for h in OWN_CHAT_HINTS):
                continue

        if not (QA_RE.search(title) or QA_RE.search(text) or AUTOMATION_RE.search(text)):
            continue
        if NONFIT_TITLE_RE.search(title):
            if not (AUTOMATION_GIG_RE.search(title + "\n" + text) and SHORT_GIG_RE.search(title + "\n" + text)):
                continue
        if NONTECH_RE.search(title) and (not TECH_CONTEXT_RE.search(text)):
            continue

        remote_mode = _safe(raw.get("remote_mode")).lower()
        engagement = _safe(raw.get("engagement")).lower()
        location = _safe(row.get("location"))
        if (remote_mode == "on_site") or ONSITE_RE.search(location):
            continue
        if not (remote_mode == "remote" or REMOTE_RE.search(location + "\n" + text)):
            continue
        if engagement == "long":
            continue
        if LONG_TERM_RE.search(text) and not SHORT_GIG_RE.search(text):
            continue

        emails = _extract_emails(raw, _safe(row.get("contact")))
        if _is_already_contacted(conn, _safe(row.get("lead_id")), emails):
            continue

        out.append(
            Candidate(
                lead_id=_safe(row.get("lead_id")),
                platform=_safe(row.get("platform")),
                lead_type=_safe(row.get("lead_type")),
                company=_safe(row.get("company")),
                title=title,
                location=location,
                url=url,
                contact=_safe(row.get("contact")),
                created_at=_safe(row.get("created_at")),
                remote_mode=remote_mode or "unknown",
                engagement=engagement or "unknown",
                budget=_safe(raw.get("budget")),
                emails=emails,
                text=text,
                source=_safe(row.get("source")),
            )
        )
        seen_url.add(canon_url or url)
        if len(out) >= int(candidate_limit):
            break

    return out


def _heuristic_score(c: Candidate) -> Tuple[float, Dict[str, Any]]:
    score = 0.0
    reasons: List[str] = []
    risks: List[str] = []

    txt = c.text
    title = c.title

    is_qa = bool(QA_RE.search(title) or QA_RE.search(txt))
    is_auto_gig = bool(AUTOMATION_GIG_RE.search(title + "\n" + txt))

    if QA_RE.search(title):
        score += 4
        reasons.append("qa_in_title")
    elif QA_RE.search(txt):
        score += 2
        reasons.append("qa_in_text")
    else:
        if is_auto_gig:
            score += 1.5
            reasons.append("automation_gig_nonqa")
        else:
            score -= 6
            risks.append("weak_qa_signal")

    auto_hits = len(AUTOMATION_RE.findall(txt))
    if auto_hits:
        score += min(3, auto_hits * 0.6)
        reasons.append("automation_tools_match")

    if c.engagement == "gig":
        score += 3
        reasons.append("gig_engagement")
    elif c.engagement == "long":
        score -= 5
        risks.append("long_engagement")

    if SHORT_GIG_RE.search(txt):
        score += 2
        reasons.append("short_task_language")
    if LONG_TERM_RE.search(txt):
        score -= 4
        risks.append("long_term_language")

    if c.remote_mode == "remote":
        score += 2
        reasons.append("remote")
    elif c.remote_mode == "hybrid":
        score += 0.5
        reasons.append("hybrid")
    elif c.remote_mode == "on_site":
        score -= 6
        risks.append("onsite")

    if c.emails:
        score += 3
        reasons.append("direct_email")
    elif c.platform in {"workana.com", "freelancermap.com"}:
        score += 1.5
        reasons.append("direct_platform_apply")
    else:
        score += 0.5

    if NONTECH_RE.search(c.company) and (not TECH_CONTEXT_RE.search(txt)):
        score -= 8
        risks.append("domain_mismatch")
    elif TECH_CONTEXT_RE.search(txt):
        score += 1.0
        reasons.append("tech_context")

    if NONFIT_TITLE_RE.search(title) and not (AUTOMATION_GIG_RE.search(txt) and SHORT_GIG_RE.search(txt)):
        score -= 7
        risks.append("title_not_fit")

    score_0_10 = max(0.0, min(10.0, (score + 8.0) / 2.0))
    meta = {
        "heuristic_raw": round(score, 2),
        "heuristic_0_10": round(score_0_10, 2),
        "reasons": reasons,
        "risks": risks,
    }
    return score_0_10, meta


def _is_probable_short_oneoff(c: Candidate) -> bool:
    txt = "\n".join(
        [
            _safe(c.title),
            _safe(c.location),
            _safe(c.budget),
            _safe(c.text),
            _safe(c.source),
            _safe(c.engagement),
        ]
    )
    if _safe(c.engagement).lower() == "gig":
        return True
    if SHORT_GIG_RE.search(txt):
        return True
    if ONEOFF_HINT_RE.search(txt):
        return True
    if c.lead_type in {"post", "project"} and re.search(r"\b(freelance|contract|paid)\b", txt, re.IGNORECASE):
        return True
    return False


def _extract_json_object(text: str) -> Dict[str, Any]:
    s = _safe(text)
    if not s:
        return {}
    try:
        obj = json.loads(s)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        pass
    start = s.find("{")
    end = s.rfind("}")
    if start >= 0 and end > start:
        try:
            obj = json.loads(s[start : end + 1])
            return obj if isinstance(obj, dict) else {}
        except Exception:
            return {}
    return {}


def _chat_completion(
    *,
    api_base: str,
    api_key: str,
    model: str,
    prompt: str,
    timeout_sec: float,
) -> Dict[str, Any]:
    url = api_base.rstrip("/") + "/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": model,
        "temperature": 0.1,
        "messages": [
            {
                "role": "system",
                "content": (
                    "You are a strict technical recruiter assistant. "
                    "Output JSON only. No markdown. Be conservative."
                ),
            },
            {"role": "user", "content": prompt},
        ],
    }
    r = requests.post(url, headers=headers, json=payload, timeout=(8.0, timeout_sec))
    r.raise_for_status()
    data = r.json()
    try:
        content = data["choices"][0]["message"]["content"]
    except Exception:
        return {}
    return _extract_json_object(_safe(content))


def _build_council_prompt(cands: Sequence[Dict[str, Any]]) -> str:
    compact = {
        "candidate_profile": {
            "role": "QA / Test Automation Engineer",
            "core_stack": [
                "manual+automation QA",
                "API testing REST/GraphQL",
                "C#/.NET",
                "NUnit/RestSharp",
                "Selenium/Playwright",
            ],
            "also_valid_gig_types": [
                "workflow automation (n8n/zapier)",
                "API integrations and automation scripts",
                "CI/CD test automation setup",
            ],
            "target": "short gigs 1-2 days, remote, globally",
            "hard_rules": [
                "reject clear non-tech/domain mismatch",
                "prefer direct contact path (email or direct client apply)",
                "reject long-term/full-time disguised as gig",
            ],
        },
        "candidates": cands,
        "required_output": {
            "scores": [
                {
                    "id": "lead_id",
                    "fit": "0-10",
                    "gig_speed": "0-10",
                    "contactability": "0-10",
                    "domain_alignment": "0-10",
                    "overall": "0-10",
                    "decision": "keep|drop",
                    "risk_flags": ["strings"],
                    "reason": "short reason",
                }
            ]
        },
    }
    return (
        "Evaluate candidates for short remote gigs. "
        "Return strict JSON object with key 'scores'. "
        "No text outside JSON.\n\n"
        + json.dumps(compact, ensure_ascii=False)
    )


def _council_scores(
    *,
    candidates: Sequence[Dict[str, Any]],
    timeout_sec: float,
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    openai_key = _safe(os.getenv("OPENAI_API_KEY"))
    xai_key = _safe(os.getenv("XAI_API_KEY"))
    council_models_raw = _safe(os.getenv("COUNCIL_MODELS")) or "openai/gpt-4.1,xai/grok-4-latest"
    council_models = [m.strip() for m in council_models_raw.split(",") if m.strip()]

    openai_model = "gpt-4.1"
    xai_model = "grok-4-latest"
    for m in council_models:
        if m.startswith("openai/"):
            openai_model = m.split("/", 1)[1].strip() or openai_model
        if m.startswith("xai/") or m.startswith("x-ai/"):
            xai_model = m.split("/", 1)[1].strip() or xai_model

    prompt = _build_council_prompt(candidates)

    openai_scores: Dict[str, Dict[str, Any]] = {}
    xai_scores: Dict[str, Dict[str, Any]] = {}

    if openai_key:
        openai_tried = [openai_model, "gpt-4o-mini"]
        last_err = ""
        for m in openai_tried:
            try:
                obj = _chat_completion(
                    api_base="https://api.openai.com/v1",
                    api_key=openai_key,
                    model=m,
                    prompt=prompt,
                    timeout_sec=timeout_sec,
                )
                for it in (obj.get("scores") or []):
                    if not isinstance(it, dict):
                        continue
                    cid = _safe(it.get("id"))
                    if cid:
                        openai_scores[cid] = it
                if openai_scores:
                    break
            except Exception as e:
                last_err = f"{type(e).__name__}: {str(e)[:220]}"
                continue
        if not openai_scores and last_err:
            print(f"[gig-hunt] openai council unavailable: {last_err}")

    if xai_key:
        try:
            obj = _chat_completion(
                api_base="https://api.x.ai/v1",
                api_key=xai_key,
                model=xai_model,
                prompt=prompt,
                timeout_sec=timeout_sec,
            )
            for it in (obj.get("scores") or []):
                if not isinstance(it, dict):
                    continue
                cid = _safe(it.get("id"))
                if cid:
                    xai_scores[cid] = it
        except Exception as e:
            xai_scores = {}
            print(f"[gig-hunt] xai council unavailable: {type(e).__name__}: {str(e)[:220]}")

    return openai_scores, xai_scores


def _to_float_0_10(value: Any) -> float:
    try:
        x = float(value)
    except Exception:
        return 0.0
    return max(0.0, min(10.0, x))


def _merge_and_rank(
    scored: Sequence[Dict[str, Any]],
    *,
    openai_scores: Dict[str, Dict[str, Any]],
    xai_scores: Dict[str, Dict[str, Any]],
    limit: int,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in scored:
        lead_id = _safe(row.get("lead_id"))
        heur = _to_float_0_10(row.get("heuristic_0_10"))

        oscore = openai_scores.get(lead_id, {})
        xscore = xai_scores.get(lead_id, {})
        o_overall = _to_float_0_10(oscore.get("overall"))
        x_overall = _to_float_0_10(xscore.get("overall"))
        o_domain = _to_float_0_10(oscore.get("domain_alignment"))
        x_domain = _to_float_0_10(xscore.get("domain_alignment"))

        components: List[Tuple[float, float]] = [(heur, 0.45)]
        if o_overall > 0:
            components.append((o_overall, 0.30))
        if x_overall > 0:
            components.append((x_overall, 0.25))
        wsum = sum(w for _, w in components) or 1.0
        final = sum(v * w for v, w in components) / wsum

        hard_drop = False
        for s in (oscore, xscore):
            if not s:
                continue
            decision = _safe(s.get("decision")).lower()
            if decision == "drop" and _to_float_0_10(s.get("domain_alignment")) <= 3.5:
                hard_drop = True
                break
        if (o_domain and o_domain <= 2.5) or (x_domain and x_domain <= 2.5):
            hard_drop = True

        merged = dict(row)
        merged["openai_overall"] = round(o_overall, 2)
        merged["xai_overall"] = round(x_overall, 2)
        merged["openai_reason"] = _safe(oscore.get("reason"))
        merged["xai_reason"] = _safe(xscore.get("reason"))
        merged["openai_decision"] = _safe(oscore.get("decision"))
        merged["xai_decision"] = _safe(xscore.get("decision"))
        merged["final_score"] = round(final, 3)
        merged["hard_drop"] = hard_drop
        out.append(merged)

    primary = [r for r in out if not bool(r.get("hard_drop"))]
    backup = [r for r in out if bool(r.get("hard_drop"))]

    primary.sort(key=lambda x: (float(x.get("final_score") or 0), float(x.get("heuristic_raw") or 0)), reverse=True)
    if len(primary) >= int(limit):
        return primary[: int(limit)]

    backup.sort(key=lambda x: (float(x.get("heuristic_0_10") or 0), float(x.get("heuristic_raw") or 0)), reverse=True)
    for b in backup:
        if len(primary) >= int(limit):
            break
        heur = float(b.get("heuristic_0_10") or 0)
        risks = _safe(b.get("heuristic_risks")).lower()
        if heur < 6.0:
            continue
        if ("domain_mismatch" in risks) or ("title_not_fit" in risks):
            continue
        b2 = dict(b)
        b2["openai_reason"] = (_safe(b2.get("openai_reason")) + " | fallback_by_heuristic").strip(" |")
        primary.append(b2)

    return primary[: int(limit)]


def _write_csv(path: Path, rows: Sequence[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "rank",
        "final_score",
        "heuristic_raw",
        "heuristic_0_10",
        "openai_overall",
        "xai_overall",
        "platform",
        "lead_type",
        "company",
        "title",
        "location",
        "engagement",
        "remote_mode",
        "budget",
        "contact_method",
        "contact_target",
        "url",
        "heuristic_reasons",
        "heuristic_risks",
        "openai_decision",
        "xai_decision",
        "openai_reason",
        "xai_reason",
        "short_signal",
        "lead_id",
        "source",
        "created_at",
    ]
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for idx, r in enumerate(rows, start=1):
            row = dict(r)
            row["rank"] = idx
            w.writerow({k: row.get(k, "") for k in fields})


def _write_json(path: Path, rows: Sequence[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(list(rows), ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Select top short remote gigs (1-2 days) using heuristic + GPT/Grok council sanity checks."
    )
    ap.add_argument("--config", default="config/config.yaml")
    ap.add_argument("--db", default="")
    ap.add_argument(
        "--platforms",
        default="workana.com,freelancermap.com,remoteok_api,jobicy_api,remotive_api,arbeitnow_api",
        help="Comma-separated platforms from leads.platform",
    )
    ap.add_argument(
        "--lead-types",
        default="project,job,post",
        help="Comma-separated lead types from leads.lead_type",
    )
    ap.add_argument("--candidate-limit", type=int, default=80)
    ap.add_argument("--limit", type=int, default=10)
    ap.add_argument(
        "--min-heuristic",
        type=float,
        default=4.0,
        help="Drop rows below this heuristic 0..10 threshold before council stage.",
    )
    ap.add_argument(
        "--strict-oneoff",
        action="store_true",
        help="Require explicit short gig/one-off signal (reduces regular job-listing noise).",
    )
    ap.add_argument(
        "--prefer-posts",
        action="store_true",
        help="Bias ranking toward lead_type post/project and penalize plain job listings.",
    )
    ap.add_argument("--out-csv", default="")
    ap.add_argument("--out-json", default="")
    ap.add_argument("--no-council", action="store_true")
    ap.add_argument("--council-timeout-sec", type=float, default=70.0)
    ap.add_argument("--telegram", action="store_true")
    args = ap.parse_args()

    load_env_file(ROOT / ".env.accounts")
    load_env_file(ROOT / ".env")

    cfg_path = resolve_path(ROOT, args.config)
    cfg = load_config(str(cfg_path)) if cfg_path.exists() else {}
    db_path = resolve_path(ROOT, str(cfg_get(cfg, "activity.db_path", "data/out/activity.sqlite")))
    if _safe(args.db):
        db_path = resolve_path(ROOT, _safe(args.db))

    platforms = [p.strip() for p in _safe(args.platforms).split(",") if p.strip()]
    lead_types = [p.strip() for p in _safe(args.lead_types).split(",") if p.strip()]
    conn = db_connect(db_path)
    try:
        init_db(conn)
        cands = _fetch_candidates(
            conn,
            platforms=platforms,
            lead_types=lead_types,
            candidate_limit=int(args.candidate_limit),
        )
    finally:
        conn.close()

    if not cands:
        print("[gig-hunt] no candidates after hard filters.")
        return 0

    scored_rows: List[Dict[str, Any]] = []
    for c in cands:
        if args.strict_oneoff and (not _is_probable_short_oneoff(c)):
            continue

        heur_0_10, meta = _heuristic_score(c)

        if args.prefer_posts:
            lead_type = _safe(c.lead_type).lower()
            if lead_type == "post":
                heur_0_10 = min(10.0, heur_0_10 + 0.8)
                meta["heuristic_raw"] = round(float(meta.get("heuristic_raw", 0.0)) + 1.0, 2)
                meta["reasons"] = list(meta.get("reasons") or []) + ["lead_type_post"]
            elif lead_type == "project":
                heur_0_10 = min(10.0, heur_0_10 + 0.4)
                meta["heuristic_raw"] = round(float(meta.get("heuristic_raw", 0.0)) + 0.5, 2)
                meta["reasons"] = list(meta.get("reasons") or []) + ["lead_type_project"]
            elif lead_type == "job":
                heur_0_10 = max(0.0, heur_0_10 - 0.9)
                meta["heuristic_raw"] = round(float(meta.get("heuristic_raw", 0.0)) - 1.2, 2)
                meta["risks"] = list(meta.get("risks") or []) + ["lead_type_job"]

        if heur_0_10 < float(args.min_heuristic):
            continue
        if c.emails:
            contact_method = "email"
            contact_target = c.emails[0]
        elif _safe(c.platform).lower() == "telegram":
            tg_handle = _extract_tg_handle(c.contact, c.url)
            if tg_handle:
                contact_method = "telegram_dm"
                contact_target = tg_handle
            else:
                contact_method = "platform_apply"
                contact_target = c.url
        else:
            contact_method = "platform_apply"
            contact_target = c.url
        scored_rows.append(
            {
                "lead_id": c.lead_id,
                "platform": c.platform,
                "lead_type": c.lead_type,
                "company": c.company,
                "title": c.title,
                "location": c.location,
                "url": c.url,
                "engagement": c.engagement,
                "remote_mode": c.remote_mode,
                "budget": c.budget,
                "contact_method": contact_method,
                "contact_target": contact_target,
                "source": c.source,
                "created_at": c.created_at,
                "heuristic_raw": meta["heuristic_raw"],
                "heuristic_0_10": round(heur_0_10, 2),
                "heuristic_reasons": ",".join(meta["reasons"]),
                "heuristic_risks": ",".join(meta["risks"]),
                "short_signal": "1" if _is_probable_short_oneoff(c) else "0",
                "snapshot_text": c.text[:1000],
            }
        )

    if not scored_rows:
        print("[gig-hunt] no candidates passed scoring threshold.")
        return 0

    scored_rows.sort(key=lambda x: (float(x.get("heuristic_0_10") or 0), float(x.get("heuristic_raw") or 0)), reverse=True)
    council_input = scored_rows[: min(len(scored_rows), max(20, int(args.limit) * 2))]
    council_payload = [
        {
            "id": r["lead_id"],
            "platform": r["platform"],
            "lead_type": r.get("lead_type", ""),
            "company": r["company"],
            "title": r["title"],
            "location": r["location"],
            "engagement": r["engagement"],
            "remote_mode": r["remote_mode"],
            "contact_method": r["contact_method"],
            "budget": r["budget"],
            "url": r["url"],
            "text": r["snapshot_text"],
        }
        for r in council_input
    ]

    openai_scores: Dict[str, Dict[str, Any]] = {}
    xai_scores: Dict[str, Dict[str, Any]] = {}
    if not args.no_council:
        openai_scores, xai_scores = _council_scores(
            candidates=council_payload,
            timeout_sec=float(args.council_timeout_sec),
        )

    merged = _merge_and_rank(
        council_input,
        openai_scores=openai_scores,
        xai_scores=xai_scores,
        limit=int(args.limit),
    )

    stamp = _now_stamp()
    out_csv = resolve_path(ROOT, args.out_csv) if _safe(args.out_csv) else (ROOT / "data" / "out" / f"gig_hunt_top_{stamp}.csv")
    out_json = resolve_path(ROOT, args.out_json) if _safe(args.out_json) else (ROOT / "data" / "out" / f"gig_hunt_top_{stamp}.json")

    _write_csv(out_csv, merged)
    _write_json(out_json, merged)

    print(f"[gig-hunt] candidates_in={len(cands)} scored={len(scored_rows)} final={len(merged)}")
    print(f"[gig-hunt] council: openai={len(openai_scores)} xai={len(xai_scores)}")
    print(f"[gig-hunt] csv: {out_csv}")
    print(f"[gig-hunt] json: {out_json}")

    for i, r in enumerate(merged, start=1):
        print(
            f"{i:02d}. {r.get('platform')} | {r.get('company')} | {r.get('title')} | "
            f"score={r.get('final_score')} | contact={r.get('contact_method')} | {r.get('url')}"
        )

    if args.telegram:
        lines = [
            "AIJobSearcher: gig hunt shortlist",
            f"Final picks: {len(merged)}",
            f"CSV: {out_csv}",
            f"JSON: {out_json}",
        ]
        send_telegram_message("\n".join(lines))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
