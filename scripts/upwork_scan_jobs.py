import argparse
import asyncio
import csv
import json
import os
import random
import re
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple
from urllib.parse import urlencode, urljoin

from bs4 import BeautifulSoup
from playwright.async_api import Page, async_playwright
from pypdf import PdfReader

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
except Exception:
    pass

from src.activity_db import LeadUpsert, add_event, connect as db_connect, init_db, upsert_lead_with_flag  # noqa: E402
from src.config import cfg_get, load_config, resolve_path  # noqa: E402
from src.email_sender import load_env_file  # noqa: E402
from src.linkedin_playwright import bool_env  # noqa: E402
from src.telegram_notify import send_telegram_message  # noqa: E402


UPWORK_BASE = "https://www.upwork.com"
UPWORK_FIND_WORK = "https://www.upwork.com/nx/find-work/"
UPWORK_CATEGORY_BASE = "https://www.upwork.com/freelance-jobs/"

EMAIL_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.IGNORECASE)
JOB_ID_RE = re.compile(r"_~(\d+)", re.IGNORECASE)

QA_RE = re.compile(
    r"\b(qa|quality\s+assurance|tester|testing|test\s*automation|automation\s*test|sdet|quality\s*engineer|test\s*engineer)\b",
    re.IGNORECASE,
)
NONFIT_RE = re.compile(
    r"\b(data\s*entry|voice\s*over|translation|cold\s*calling|telemarketing|bookkeeping|transcription)\b",
    re.IGNORECASE,
)
LONG_HINT_RE = re.compile(
    r"\b(more than 6 months|3 to 6 months|1 to 3 months|contract to hire|full[-\s]?time|ongoing)\b",
    re.IGNORECASE,
)
GIG_HINT_RE = re.compile(
    r"\b(less than 1 month|quick|urgent|single task|one[-\s]?time|few hours|small fix)\b",
    re.IGNORECASE,
)
GEO_RESTRICT_RE = re.compile(
    r"\b("
    r"us[\s-]?based|u\.s\.\s*based|united states based|"
    r"us only|u\.s\. only|usa only|united states only|"
    r"must be based in (the )?(us|u\.s\.|usa|united states)|"
    r"for (morocco|argentina|brazil|europe|eu|uk|united kingdom|canada|australia)|"
    r"based in (morocco|argentina|brazil|europe|eu|uk|united kingdom|canada|australia)|"
    r"local candidates only|residents only|citizens only|local payments"
    r")\b",
    re.IGNORECASE,
)
VR_REQUIRED_RE = re.compile(
    r"\b(meta\s*quest|oculus|vr\s*(headset|device|game)|virtual reality)\b",
    re.IGNORECASE,
)

DEFAULT_CATEGORIES = [
    "software-qa-testing",
    "qa",
    "web-testing",
    "bug-reports",
    "api-development",
    "web-scraping",
    "python-script",
    "powershell",
    "selenium-webdriver",
    "playwright",
    "openai",
    "ai-development",
]


@dataclass
class ScannedJob:
    platform: str
    job_id: str
    url: str
    title: str
    location: str
    remote_mode: str
    engagement: str
    payment_model: str
    duration: str
    workload: str
    posted_at: str
    budget_text: str
    budget_min: Optional[float]
    budget_max: Optional[float]
    currency: str
    emails: List[str]
    skills: List[str]
    score: int
    reasons: List[str]
    description: str
    raw: Dict[str, Any]


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _extract_resume_text(pdf_path: Path) -> str:
    try:
        reader = PdfReader(str(pdf_path))
    except Exception:
        return ""
    parts: List[str] = []
    for page in reader.pages:
        try:
            parts.append(page.extract_text() or "")
        except Exception:
            continue
    return "\n".join(parts)


def _resume_skill_tokens(text: str) -> List[str]:
    t = (text or "").lower()
    candidates = [
        "qa",
        "c#",
        ".net",
        "nunit",
        "restsharp",
        "rest",
        "graphql",
        "api",
        "postman",
        "sql",
        "selenium",
        "playwright",
        "jira",
        "jenkins",
        "bitbucket",
        "gitlab",
        "docker",
        "security",
        "regression",
        "exploratory",
        "automation",
        "telegram",
        "llm",
        "openai",
        "webhook",
        "zapier",
        "n8n",
    ]
    out: List[str] = []
    seen: Set[str] = set()
    for tok in candidates:
        if tok in t and tok not in seen:
            seen.add(tok)
            out.append(tok)
    return out


def _category_url(slug: str, page: int) -> str:
    base = urljoin(UPWORK_CATEGORY_BASE, f"{slug.strip().strip('/')}/")
    if page <= 1:
        return base
    return base + "?" + urlencode({"page": str(page)})


def _normalize_apply_url(href: str) -> str:
    h = str(href or "").strip()
    if not h:
        return ""
    if h.startswith("/"):
        h = urljoin(UPWORK_BASE, h)
    if not h.startswith("http"):
        return ""
    h = h.split("#", 1)[0]
    h = h.split("?", 1)[0]
    if not h.endswith("/"):
        h += "/"
    return h


def _extract_job_id(url: str) -> str:
    m = JOB_ID_RE.search(url or "")
    if not m:
        return ""
    return m.group(1)


def _extract_emails(text: str) -> List[str]:
    if not text:
        return []
    return sorted({m.group(0).lower() for m in EMAIL_RE.finditer(text)})[:10]


def _parse_budget(ld_job: Dict[str, Any]) -> Tuple[str, Optional[float], Optional[float], str, str]:
    budget_text = ""
    bmin = None
    bmax = None
    currency = ""
    payment_model = "unknown"

    bs = ld_job.get("baseSalary")
    if not isinstance(bs, dict):
        return budget_text, bmin, bmax, currency, payment_model

    currency = str(bs.get("currency") or "").strip()
    val = bs.get("value")
    if isinstance(val, dict):
        try:
            bmin = float(val.get("minValue")) if val.get("minValue") is not None else None
        except Exception:
            bmin = None
        try:
            bmax = float(val.get("maxValue")) if val.get("maxValue") is not None else None
        except Exception:
            bmax = None
        unit = str(val.get("unitText") or "").strip().upper()
        if unit == "HOUR":
            payment_model = "hourly"
        elif unit:
            payment_model = unit.lower()

    cur = currency or "USD"
    if bmin is not None and bmax is not None:
        if payment_model == "hourly":
            budget_text = f"{cur} {bmin:g}-{bmax:g}/hr"
        else:
            budget_text = f"{cur} {bmin:g}-{bmax:g}"
    elif bmin is not None:
        if payment_model == "hourly":
            budget_text = f"{cur} {bmin:g}/hr"
        else:
            budget_text = f"{cur} {bmin:g}"

    return budget_text, bmin, bmax, currency, payment_model


def _extract_duration_and_workload(text: str) -> Tuple[str, str]:
    duration = ""
    workload = ""
    dur_match = re.search(r"\b(Less than 1 month|1 to 3 months|3 to 6 months|More than 6 months)\b", text, re.IGNORECASE)
    if dur_match:
        duration = dur_match.group(1).strip()
    wl_match = re.search(
        r"\b(Less than 30 hrs/week|More than 30 hrs/week|30\+ hrs/week|[0-9]{1,2}\s*to\s*[0-9]{1,2}\s*hrs/week)\b",
        text,
        re.IGNORECASE,
    )
    if wl_match:
        workload = wl_match.group(1).strip()
    return duration, workload


def _extract_location(text: str, ld_job: Dict[str, Any]) -> str:
    if re.search(r"\bWorldwide\b", text, re.IGNORECASE):
        return "Worldwide"

    m = re.search(r"\b(United States|Australia|Canada|United Kingdom|Europe|Asia|Remote)\s+only\b", text, re.IGNORECASE)
    if m:
        return m.group(0).strip()

    job_loc = ld_job.get("jobLocation")
    if isinstance(job_loc, dict):
        addr = job_loc.get("address")
        if isinstance(addr, dict):
            c = str(addr.get("addressCountry") or "").strip()
            if c:
                return c

    return "Unknown"


def _extract_skills(body_text: str) -> List[str]:
    m = re.search(r"Skills and Expertise\s+(.*?)\s+Activity on this job", body_text, re.IGNORECASE | re.DOTALL)
    if not m:
        return []
    block = m.group(1)
    out: List[str] = []
    for raw in block.splitlines():
        s = raw.strip()
        if not s:
            continue
        low = s.lower()
        if low in {"mandatory skills", "preferred qualifications"}:
            continue
        if len(s) > 64:
            continue
        if ":" in s:
            continue
        out.append(s)
    seen: Set[str] = set()
    uniq: List[str] = []
    for s in out:
        k = s.lower()
        if k in seen:
            continue
        seen.add(k)
        uniq.append(s)
    return uniq[:25]


def _extract_summary(text: str, ld_job: Dict[str, Any]) -> str:
    desc = str(ld_job.get("description") or "").strip()
    if desc:
        parsed = BeautifulSoup(desc, "html.parser").get_text("\n", strip=True)
        if parsed:
            return parsed

    m = re.search(
        r"Summary\s+(.*?)\s+(Project Type|Skills and Expertise|Activity on this job|About the client)",
        text,
        re.IGNORECASE | re.DOTALL,
    )
    if m:
        return m.group(1).strip()
    return ""


def _classify_engagement(*, duration: str, workload: str, payment_model: str, text: str, budget_max: Optional[float]) -> str:
    low = (text or "").lower()
    dlow = (duration or "").lower()
    wlow = (workload or "").lower()
    model = (payment_model or "").lower()

    if "more than 6 months" in dlow or "3 to 6 months" in dlow or "1 to 3 months" in dlow:
        return "long"
    if "less than 1 month" in dlow:
        return "gig"
    if "contract to hire" in low:
        return "long"
    if model == "hourly" and "less than 30 hrs/week" in wlow:
        return "gig"
    if model == "hourly" and "more than 30 hrs/week" in wlow:
        return "long"
    if model != "hourly" and budget_max is not None and budget_max <= 300:
        return "gig"
    if LONG_HINT_RE.search(low):
        return "long"
    if GIG_HINT_RE.search(low):
        return "gig"
    return "unknown"


def _score_job(
    *,
    title: str,
    text: str,
    resume_tokens: List[str],
    location: str,
    remote_mode: str,
    engagement: str,
    payment_model: str,
    budget_min: Optional[float],
) -> Tuple[int, List[str]]:
    low = (text or "").lower()
    score = 0
    reasons: List[str] = []

    if QA_RE.search(title or ""):
        score += 8
        reasons.append("qa_in_title")
    elif QA_RE.search(low):
        score += 4
        reasons.append("qa_in_text")
    else:
        score -= 6
        reasons.append("not_qa_like")

    if remote_mode == "remote":
        score += 2
        reasons.append("remote")
    if "worldwide" in (location or "").lower():
        score += 2
        reasons.append("worldwide")

    token_hits = 0
    for tok in resume_tokens:
        if tok in low:
            score += 1
            token_hits += 1
    if token_hits:
        reasons.append(f"resume_tokens:{token_hits}")

    if engagement == "long":
        score += 2
        reasons.append("long_term_signal")
    elif engagement == "gig":
        score += 1
        reasons.append("gig_signal")

    if payment_model == "hourly":
        score += 1
        reasons.append("hourly")
        if budget_min is not None and budget_min >= 15:
            score += 1
            reasons.append("hourly_rate_ge_15")

    if NONFIT_RE.search(low) and not QA_RE.search(low):
        score -= 6
        reasons.append("nonfit_content")

    return score, reasons


def _hard_block_reason(row: ScannedJob) -> str:
    blob = "\n".join(
        [
            row.title or "",
            row.description or "",
            row.location or "",
            row.duration or "",
            row.workload or "",
            " ".join(row.skills or []),
        ]
    )
    low = blob.lower()

    if GEO_RESTRICT_RE.search(low):
        return "geo_restricted"
    if VR_REQUIRED_RE.search(low):
        return "vr_required"
    return ""


def _split_long_gig(rows: List[ScannedJob]) -> Tuple[List[ScannedJob], List[ScannedJob]]:
    long_rows: List[ScannedJob] = []
    gig_rows: List[ScannedJob] = []
    for r in rows:
        if r.engagement == "long":
            long_rows.append(r)
        else:
            gig_rows.append(r)
    return long_rows, gig_rows


def _write_csv(path: Path, rows: Iterable[ScannedJob]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "captured_at",
                "platform",
                "job_id",
                "score",
                "remote_mode",
                "engagement",
                "payment_model",
                "duration",
                "workload",
                "title",
                "location",
                "budget",
                "posted_at",
                "url",
                "contact_emails",
                "skills",
                "reasons",
            ],
        )
        w.writeheader()
        now = _now_iso()
        for r in rows:
            w.writerow(
                {
                    "captured_at": now,
                    "platform": r.platform,
                    "job_id": r.job_id,
                    "score": r.score,
                    "remote_mode": r.remote_mode,
                    "engagement": r.engagement,
                    "payment_model": r.payment_model,
                    "duration": r.duration,
                    "workload": r.workload,
                    "title": r.title,
                    "location": r.location,
                    "budget": r.budget_text,
                    "posted_at": r.posted_at,
                    "url": r.url,
                    "contact_emails": ";".join(r.emails),
                    "skills": ";".join(r.skills),
                    "reasons": ",".join(r.reasons),
                }
            )


def _print_favorites(tag: str, rows: Sequence[ScannedJob], limit: int) -> None:
    print(f"[upwork-scan] favorites {tag}: {min(limit, len(rows))}")
    for idx, r in enumerate(rows[:limit], start=1):
        print(f"{idx}. [{r.score}] {r.title} :: {r.url}")


async def _collect_links_from_category(
    page: Page,
    *,
    slug: str,
    pages: int,
    min_delay_sec: float,
    max_delay_sec: float,
) -> Tuple[List[str], int]:
    out: List[str] = []
    blocked = 0

    for pnum in range(1, max(1, pages) + 1):
        url = _category_url(slug, pnum)
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=90_000)
        except Exception as e:
            print(f"[upwork-scan] category={slug} page={pnum} nav_error={e}")
            continue

        await page.wait_for_timeout(int(random.uniform(min_delay_sec, max_delay_sec) * 1000))
        try:
            title = (await page.title()) or ""
        except Exception as e:
            print(f"[upwork-scan] category={slug} page={pnum} title_error={e}")
            continue
        try:
            body_txt = await page.inner_text("body")
        except Exception:
            body_txt = ""
        if "Just a moment" in title or "Challenge - Upwork" in title:
            blocked += 1
            print(f"[upwork-scan] category={slug} page={pnum} blocked_by_challenge")
            continue
        if re.search(r"\bWe can't find this page\b", body_txt, re.IGNORECASE):
            print(f"[upwork-scan] category={slug} page={pnum} no_page")
            continue

        hrefs = await page.eval_on_selector_all(
            "a[href*='/freelance-jobs/apply/']",
            "els => els.map(e => e.getAttribute('href'))",
        )
        page_urls: List[str] = []
        for h in hrefs:
            n = _normalize_apply_url(str(h or ""))
            if n:
                page_urls.append(n)

        if not page_urls:
            html_text = await page.content()
            for m in re.finditer(r"(/freelance-jobs/apply/[^\"'<>\\s]+)", html_text):
                n = _normalize_apply_url(m.group(1))
                if n:
                    page_urls.append(n)

        dedup_page = list(dict.fromkeys(page_urls))
        out.extend(dedup_page)
        print(f"[upwork-scan] category={slug} page={pnum} links={len(dedup_page)}")

    return list(dict.fromkeys(out)), blocked


def _extract_jobposting_ld(html_text: str) -> Dict[str, Any]:
    soup = BeautifulSoup(html_text or "", "html.parser")
    for sc in soup.find_all("script", attrs={"type": "application/ld+json"}):
        raw = (sc.string or sc.get_text() or "").strip()
        if not raw:
            continue
        try:
            obj = json.loads(raw)
        except Exception:
            continue
        if isinstance(obj, dict) and str(obj.get("@type") or "").lower() == "jobposting":
            return obj
    return {}


async def _scan_job_page(
    page: Page,
    *,
    url: str,
    resume_tokens: List[str],
    min_delay_sec: float,
    max_delay_sec: float,
) -> Optional[ScannedJob]:
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=90_000)
    except Exception as e:
        print(f"[upwork-scan] job_nav_error url={url} error={e}")
        return None

    await page.wait_for_timeout(int(random.uniform(min_delay_sec, max_delay_sec) * 1000))
    title_tag = (await page.title()) or ""
    page_url = page.url
    try:
        body_text = await page.inner_text("body")
    except Exception:
        body_text = ""
    if "Just a moment" in title_tag or "Challenge - Upwork" in title_tag:
        print(f"[upwork-scan] blocked_by_challenge url={url}")
        return None
    if "/ab/account-security/login" in page_url:
        print(f"[upwork-scan] redirected_to_login url={url}")
        return None

    html_text = await page.content()
    ld_job = _extract_jobposting_ld(html_text)

    job_title = str(ld_job.get("title") or "").strip()
    if not job_title:
        h1 = ""
        try:
            h1 = await page.locator("h1").first.text_content(timeout=2_000)
        except Exception:
            h1 = ""
        job_title = str(h1 or "").strip()
    if not job_title:
        job_title = title_tag.replace(" - Upwork", "").strip()

    description = _extract_summary(body_text, ld_job)
    location = _extract_location(body_text, ld_job)
    remote_mode = "remote" if str(ld_job.get("jobLocationType") or "").upper() == "TELECOMMUTE" else "unknown"
    posted_at = str(ld_job.get("datePosted") or "").strip()
    duration, workload = _extract_duration_and_workload(f"{title_tag}\n{body_text}")
    budget_text, budget_min, budget_max, currency, payment_model = _parse_budget(ld_job)
    skills = _extract_skills(body_text)
    emails = _extract_emails(f"{description}\n{body_text}")

    full_text = "\n".join([job_title, description, "\n".join(skills), body_text])
    engagement = _classify_engagement(
        duration=duration,
        workload=workload,
        payment_model=payment_model,
        text=full_text,
        budget_max=budget_max,
    )
    score, reasons = _score_job(
        title=job_title,
        text=full_text,
        resume_tokens=resume_tokens,
        location=location,
        remote_mode=remote_mode,
        engagement=engagement,
        payment_model=payment_model,
        budget_min=budget_min,
    )

    job_id = _extract_job_id(page_url) or _extract_job_id(url)
    if not job_id:
        return None

    raw = {
        "platform_domain": "upwork.com",
        "job_id": job_id,
        "page_title": title_tag,
        "duration": duration,
        "workload": workload,
        "budget_min": budget_min,
        "budget_max": budget_max,
        "currency": currency,
        "payment_model": payment_model,
        "skills": skills,
        "score": score,
        "reasons": reasons,
        "remote_mode": remote_mode,
        "engagement": engagement,
    }

    return ScannedJob(
        platform="upwork.com",
        job_id=job_id,
        url=page_url,
        title=job_title,
        location=location,
        remote_mode=remote_mode,
        engagement=engagement,
        payment_model=payment_model,
        duration=duration,
        workload=workload,
        posted_at=posted_at,
        budget_text=budget_text,
        budget_min=budget_min,
        budget_max=budget_max,
        currency=currency,
        emails=emails,
        skills=skills,
        score=score,
        reasons=reasons,
        description=description,
        raw=raw,
    )


async def _run_scan(
    *,
    user_data_dir: Path,
    categories: List[str],
    pages_per_category: int,
    max_links_per_category: int,
    max_jobs: int,
    min_delay_sec: float,
    max_delay_sec: float,
    resume_tokens: List[str],
    min_score: int,
    strict_qa_title: bool,
    allow_geo_restricted: bool,
    allow_vr_required: bool,
    headless: bool,
) -> List[ScannedJob]:
    all_links: List[str] = []
    blocked_pages = 0

    async with async_playwright() as pw:
        ctx = await pw.chromium.launch_persistent_context(
            str(user_data_dir),
            headless=headless,
            locale="en-US",
            args=["--disable-blink-features=AutomationControlled"],
            viewport={"width": 1460, "height": 940},
        )
        page = ctx.pages[0] if ctx.pages else await ctx.new_page()
        await page.set_extra_http_headers({"Accept-Language": "en-US,en;q=0.9"})

        try:
            await page.goto(UPWORK_FIND_WORK, wait_until="domcontentloaded", timeout=90_000)
            await page.wait_for_timeout(1800)
            title = await page.title()
            if "Just a moment" in title:
                print("[upwork-scan] find-work is behind challenge in current mode; using public category scan fallback.")
            elif "/ab/account-security/login" in page.url:
                print("[upwork-scan] find-work requires login in current session; using public category scan fallback.")
            else:
                print("[upwork-scan] find-work is reachable; continuing with category feeds for broad coverage.")
        except Exception as e:
            print(f"[upwork-scan] find-work probe error: {e}")

        for slug in categories:
            links, blocked = await _collect_links_from_category(
                page,
                slug=slug,
                pages=pages_per_category,
                min_delay_sec=min_delay_sec,
                max_delay_sec=max_delay_sec,
            )
            blocked_pages += blocked
            if max_links_per_category > 0:
                links = links[:max_links_per_category]
            all_links.extend(links)
            print(f"[upwork-scan] category={slug} unique_links={len(links)}")

        dedup_links = list(dict.fromkeys(all_links))
        if max_jobs > 0:
            dedup_links = dedup_links[:max_jobs]
        print(
            f"[upwork-scan] links_total={len(all_links)} "
            f"links_unique={len(dedup_links)} blocked_pages={blocked_pages}"
        )

        scanned: List[ScannedJob] = []
        for idx, url in enumerate(dedup_links, start=1):
            try:
                row = await _scan_job_page(
                    page,
                    url=url,
                    resume_tokens=resume_tokens,
                    min_delay_sec=min_delay_sec,
                    max_delay_sec=max_delay_sec,
                )
            except Exception as e:
                print(f"[upwork-scan] job_parse_error idx={idx} url={url} error={e}")
                continue
            if not row:
                continue
            if strict_qa_title and (not QA_RE.search(row.title)):
                continue
            if row.score < min_score:
                continue
            block_reason = _hard_block_reason(row)
            if block_reason == "geo_restricted" and (not allow_geo_restricted):
                print(f"[upwork-scan] skip_geo_restricted idx={idx} title={row.title[:80]}")
                continue
            if block_reason == "vr_required" and (not allow_vr_required):
                print(f"[upwork-scan] skip_vr_required idx={idx} title={row.title[:80]}")
                continue
            scanned.append(row)
            print(
                f"[upwork-scan] job {idx}/{len(dedup_links)} "
                f"score={row.score} engagement={row.engagement or 'unknown'} title={row.title[:90]}"
            )

        await ctx.close()
        return scanned


def main() -> int:
    ap = argparse.ArgumentParser(
        description=(
            "Scan Upwork jobs using public category pages (fallback when /nx/find-work requires login/challenge), "
            "rank by CV fit, and split into long-term vs gigs."
        )
    )
    ap.add_argument("--config", default="config/config.yaml")
    ap.add_argument("--db", default="", help="Override DB path")
    ap.add_argument("--resume-pdf", default="Docs/ARTEM_BONDARENKO_CV_2026.pdf")
    ap.add_argument("--out-dir", default="data/out")
    ap.add_argument("--categories", default=",".join(DEFAULT_CATEGORIES))
    ap.add_argument("--pages-per-category", type=int, default=2)
    ap.add_argument("--max-links-per-category", type=int, default=24)
    ap.add_argument("--max-jobs", type=int, default=120)
    ap.add_argument("--min-score", type=int, default=6)
    ap.add_argument("--strict-qa-title", action="store_true")
    ap.add_argument("--allow-geo-restricted", action="store_true", help="Keep country-locked jobs (default: filtered out)")
    ap.add_argument("--allow-vr-required", action="store_true", help="Keep VR-hardware-required jobs (default: filtered out)")
    ap.add_argument("--min-delay-sec", type=float, default=2.2)
    ap.add_argument("--max-delay-sec", type=float, default=4.8)
    ap.add_argument("--user-data-dir", default="", help="Playwright persistent profile dir")
    ap.add_argument("--headless", action="store_true", help="Headless mode (often blocked by Upwork challenge)")
    ap.add_argument("--favorites", type=int, default=8, help="How many favorite links to print for each bucket")
    ap.add_argument("--write-db", action="store_true", help="Upsert leads/events into activity DB")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--telegram", action="store_true", help="Send summary via Telegram bot")
    args = ap.parse_args()

    load_env_file(ROOT / ".env")
    load_env_file(ROOT / ".env.accounts")

    cfg_path = resolve_path(ROOT, args.config)
    cfg = load_config(str(cfg_path)) if cfg_path.exists() else {}
    db_path = resolve_path(ROOT, str(cfg_get(cfg, "activity.db_path", "data/out/activity.sqlite")))
    if str(args.db).strip():
        db_path = resolve_path(ROOT, str(args.db).strip())

    user_data_env = str(os.getenv("PLAYWRIGHT_USER_DATA_DIR") or "").strip()
    user_data_dir = resolve_path(ROOT, args.user_data_dir.strip()) if str(args.user_data_dir).strip() else (
        resolve_path(ROOT, user_data_env) if user_data_env else resolve_path(ROOT, "data/profiles/default")
    )
    user_data_dir.mkdir(parents=True, exist_ok=True)

    resume_path = resolve_path(ROOT, args.resume_pdf)
    resume_text = _extract_resume_text(resume_path) if resume_path.exists() else ""
    resume_tokens = _resume_skill_tokens(resume_text)
    if resume_tokens:
        print(f"[upwork-scan] resume tokens: {', '.join(resume_tokens)}")

    categories = [s.strip().strip("/").lower() for s in str(args.categories).split(",") if s.strip()]
    categories = list(dict.fromkeys(categories))
    if not categories:
        categories = DEFAULT_CATEGORIES[:]
    print(f"[upwork-scan] categories: {', '.join(categories)}")
    print(f"[upwork-scan] user_data_dir: {user_data_dir}")
    print(f"[upwork-scan] mode: {'headless' if args.headless else 'headful'}")

    scanned = asyncio.run(
        _run_scan(
            user_data_dir=user_data_dir,
            categories=categories,
            pages_per_category=max(1, int(args.pages_per_category)),
            max_links_per_category=max(1, int(args.max_links_per_category)),
            max_jobs=max(1, int(args.max_jobs)),
            min_delay_sec=max(0.5, float(args.min_delay_sec)),
            max_delay_sec=max(float(args.min_delay_sec) + 0.1, float(args.max_delay_sec)),
            resume_tokens=resume_tokens,
            min_score=int(args.min_score),
            strict_qa_title=bool(args.strict_qa_title),
            allow_geo_restricted=bool(args.allow_geo_restricted),
            allow_vr_required=bool(args.allow_vr_required),
            headless=bool(args.headless),
        )
    )

    scanned.sort(key=lambda r: (r.score, r.posted_at), reverse=True)
    long_rows, gig_rows = _split_long_gig(scanned)

    out_dir = resolve_path(ROOT, args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_all = out_dir / f"upwork_scan_all_{stamp}.csv"
    csv_long = out_dir / f"upwork_scan_long_{stamp}.csv"
    csv_gig = out_dir / f"upwork_scan_gigs_{stamp}.csv"
    _write_csv(csv_all, scanned)
    _write_csv(csv_long, long_rows)
    _write_csv(csv_gig, gig_rows)

    print(
        f"[upwork-scan] done total={len(scanned)} "
        f"long={len(long_rows)} gigs={len(gig_rows)} min_score={args.min_score}"
    )
    print(f"[upwork-scan] wrote: {csv_all}")
    print(f"[upwork-scan] wrote: {csv_long}")
    print(f"[upwork-scan] wrote: {csv_gig}")
    _print_favorites("long", long_rows, max(1, int(args.favorites)))
    _print_favorites("gigs", gig_rows, max(1, int(args.favorites)))

    inserted = 0
    updated = 0
    if args.write_db and (not args.dry_run):
        conn = db_connect(db_path)
        init_db(conn)
        with conn:
            for r in scanned:
                lead = LeadUpsert(
                    platform=r.platform,
                    lead_type="project",
                    contact=r.job_id,
                    url=r.url,
                    company="Upwork",
                    job_title=r.title,
                    location=r.location,
                    source="public_scan:upwork_categories",
                    created_at=_now_iso(),
                    raw=r.raw,
                )
                lead_id, was_inserted = upsert_lead_with_flag(conn, lead)
                if was_inserted:
                    inserted += 1
                else:
                    updated += 1

                add_event(
                    conn,
                    lead_id=lead_id,
                    event_type="upwork_job_collected",
                    status="ok",
                    details={
                        "platform": r.platform,
                        "job_id": r.job_id,
                        "url": r.url,
                        "remote_mode": r.remote_mode,
                        "engagement": r.engagement,
                        "payment_model": r.payment_model,
                        "budget": r.budget_text,
                        "score": r.score,
                    },
                )
            conn.commit()
        try:
            conn.close()
        except Exception:
            pass

    print(f"[upwork-scan] db: {db_path} inserted={inserted} updated={updated}")

    if args.telegram and bool_env("TELEGRAM_REPORT", True):
        lines = [
            "AIJobSearcher: Upwork scan",
            f"Fit jobs: {len(scanned)} (long={len(long_rows)} gigs={len(gig_rows)})",
            f"CSV all: {csv_all}",
            f"CSV long: {csv_long}",
            f"CSV gigs: {csv_gig}",
        ]
        send_telegram_message("\n".join(lines))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
