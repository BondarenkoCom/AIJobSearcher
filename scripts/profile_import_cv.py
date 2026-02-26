import argparse
import re
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.activity_db import connect as db_connect  # noqa: E402
from src.activity_db import init_db  # noqa: E402
from src.config import cfg_get, load_config, resolve_path  # noqa: E402
from src.profile_store import insert_answer_if_missing, normalize_person_name, upsert_document, upsert_profile_kv  # noqa: E402


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="replace")


def _section(text: str, start: str, end: str) -> str:
    low = text.lower()
    s = low.find(start.lower())
    if s < 0:
        return ""
    s = s + len(start)
    e = low.find(end.lower(), s) if end else -1
    if e < 0:
        chunk = text[s:]
    else:
        chunk = text[s:e]
    return chunk.strip()


def _find_next_header(lines: List[str], start_idx: int) -> int:
    # Treat ALLCAPS lines as headers (e.g. ACHIEVEMENTS, LANGUAGES).
    hdr = re.compile(r"^[A-Z][A-Z0-9 &/]+$")
    for i in range(start_idx, len(lines)):
        t = (lines[i] or "").strip()
        if hdr.match(t):
            return i
    return len(lines)


def _parse_top(lines: List[str]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if lines:
        out["candidate.name"] = normalize_person_name((lines[0] or "").strip())
    if len(lines) > 1:
        out["candidate.title"] = (lines[1] or "").strip()

    # Contact line: "Vietnam, Ho Chi Minh City | +84 ... | email"
    if len(lines) > 2:
        parts = [p.strip() for p in (lines[2] or "").split("|")]
        if parts:
            out["candidate.location"] = parts[0]
        if len(parts) > 1:
            out["candidate.phone"] = parts[1]
        if len(parts) > 2:
            out["candidate.email"] = parts[2]

    # Links: LinkedIn / Upwork / GitHub
    blob = "\n".join(lines[:12])
    m = re.search(r"LinkedIn:\s*([^\s|]+)", blob, flags=re.IGNORECASE)
    if m:
        v = m.group(1).strip()
        if v and not v.startswith("http"):
            v = "https://" + v
        out["candidate.linkedin"] = v
    m = re.search(r"Upwork:\s*([^\s|]+)", blob, flags=re.IGNORECASE)
    if m:
        v = m.group(1).strip()
        if v and not v.startswith("http"):
            v = "https://" + v
        out["candidate.upwork"] = v
    m = re.search(r"(github\.com/\S+)", blob, flags=re.IGNORECASE)
    if m:
        v = m.group(1).strip().rstrip(").,")
        if v and not v.startswith("http"):
            v = "https://" + v
        out["candidate.github"] = v

    out["candidate.availability"] = "Immediate"
    return out


def _seed_answers(profile: Dict[str, str]) -> List[Tuple[str, str]]:
    """
    Return list of (question, answer) pairs to seed into answer_bank.
    IMPORTANT: keep truthful and CV-based. Do not seed WordPress-specific claims.
    """
    summary = (profile.get("candidate.summary") or "").strip()
    skills = (profile.get("candidate.skills") or "").strip()

    remote_ans = (
        "Yes. I have 5+ years of experience working in remote/distributed teams. "
        "Most recently I worked remotely as the sole QA owner for a web marketplace + admin portal, "
        "coordinating via Jira and CI pipelines and verifying releases end-to-end."
    )
    if summary:
        # Keep it short and consistent.
        remote_ans = remote_ans

    qa_interest = (
        "API testing (REST/GraphQL) and auth/security testing, C#/.NET automation (NUnit/RestSharp), "
        "CI/CD pipelines (Bitbucket Pipelines, Jenkins, GitLab CI, Docker), and pragmatic UI checks "
        "with Playwright/Selenium for critical flows. I also enjoy release verification and evidence-based defect reporting."
    )
    if skills:
        qa_interest = qa_interest

    web_tech = (
        "I mainly test web platforms and APIs. Technologies/tools I work with include REST and GraphQL APIs, "
        "Postman, CI/CD (Bitbucket Pipelines/Jenkins/GitLab CI), Docker, and basic SQL. "
        "I have fundamentals in HTML/CSS and focus on QA for web apps rather than building full web features."
    )

    return [
        ("Have you worked remotely with a distributed team before? If so, tell me about that experience.", remote_ans),
        ("Which areas, tools or technologies related to QA you find interesting or exciting?", qa_interest),
        ("What type of web development technologies have you worked with?", web_tech),
    ]


def main() -> int:
    ap = argparse.ArgumentParser(description="Import text CV into activity.sqlite (profile_kv + documents + seeded answers)")
    ap.add_argument("--config", default="config/config.yaml", help="Config YAML")
    ap.add_argument("--db", default="", help="Override DB path")
    ap.add_argument("--cv", default="Docs/ARTEM_BONDARENKO_textCV.txt", help="Path to text CV")
    args = ap.parse_args()

    cfg_path = resolve_path(ROOT, args.config)
    cfg = load_config(str(cfg_path)) if cfg_path.exists() else {}
    db_path = resolve_path(ROOT, str(cfg_get(cfg, "activity.db_path", "data/out/activity.sqlite")))
    if args.db.strip():
        db_path = resolve_path(ROOT, args.db.strip())

    cv_path = resolve_path(ROOT, args.cv)
    if not cv_path.exists():
        print(f"[profile] CV not found: {cv_path}")
        return 2

    raw = _read_text(cv_path)
    lines = [ln.rstrip("\r") for ln in raw.splitlines()]

    profile = _parse_top(lines)
    profile["candidate.summary"] = _section(raw, "SUMMARY", "EXPERIENCE")

    # Skills section: from "SKILLS" to next ALLCAPS header (e.g., ACHIEVEMENTS).
    low_lines = [ln.strip() for ln in lines]
    try:
        skills_idx = next(i for i, ln in enumerate(low_lines) if ln.strip().upper() == "SKILLS")
    except StopIteration:
        skills_idx = -1
    if skills_idx >= 0:
        end_idx = _find_next_header(lines, skills_idx + 1)
        skills_text = "\n".join([l.strip() for l in lines[skills_idx + 1 : end_idx]]).strip()
        profile["candidate.skills"] = skills_text

    conn = db_connect(db_path)
    init_db(conn)

    keys_written = 0
    answers_seeded = 0
    with conn:
        upsert_document(conn, doc_id="cv_text_v1", doc_type="cv_text", content=raw)

        for k, v in profile.items():
            if not (v or "").strip():
                continue
            upsert_profile_kv(conn, key=k, value=v)
            keys_written += 1

        for q, a in _seed_answers(profile):
            if insert_answer_if_missing(conn, q_raw=q, answer=a, status="confirmed"):
                answers_seeded += 1

        conn.commit()

    conn.close()
    print(f"[profile] imported CV: {cv_path.name}")
    print(f"[profile] db: {db_path}")
    print(f"[profile] profile_kv upserts: {keys_written}")
    print(f"[profile] answer_bank seeded: {answers_seeded}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
