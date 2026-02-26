import argparse
import asyncio
import json
import os
import random
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
except Exception:
    pass

from src.activity_db import add_event, connect as db_connect, init_db  # noqa: E402
from src.config import cfg_get, load_config, resolve_path  # noqa: E402
from src.email_sender import load_env_file  # noqa: E402
from src.auto_controller import AutoController  # noqa: E402
from src.linkedin_playwright import SafeCloser, dump_debug  # noqa: E402
from src.notify import notify  # noqa: E402
from src.profile_store import get_answer, insert_answer_if_missing, load_profile, normalize_question  # noqa: E402

QA_TITLE_RE = re.compile(
    r"\b(qa|quality\s*assurance|sdet|test\s*automation|automation\s*engineer|test\s*engineer|software\s*tester|tester)\b",
    re.IGNORECASE,
)


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _parse_json(raw_json: Optional[str]) -> Dict[str, Any]:
    if not raw_json:
        return {}
    try:
        v = json.loads(raw_json)
        return v if isinstance(v, dict) else {}
    except Exception:
        return {}


def _domain(url: str) -> str:
    try:
        return (urlparse(url).netloc or "").lower()
    except Exception:
        return ""


def _is_linkedin_url(url: str) -> bool:
    return "linkedin.com" in _domain(url)


def _split_name(full_name: str) -> Tuple[str, str]:
    parts = [p for p in (full_name or "").strip().split() if p]
    if not parts:
        return ("", "")
    if len(parts) == 1:
        return (parts[0], "")
    return (parts[0], " ".join(parts[1:]))


def _clean_question_text(text: str) -> str:
    t = str(text or "").replace("\r", "\n")
    lines = [ln.strip() for ln in t.split("\n")]
    lines = [ln for ln in lines if ln and ln.lower() not in {"required", "* required fields"}]
    if not lines:
        return ""
    out: List[str] = []
    for ln in lines:
        if not out or out[-1].lower() != ln.lower():
            out.append(ln)
    t = " ".join(out)
    t = re.sub(r"\s+", " ", t).strip()
    return t.strip(" :|-")


def _is_sensitive_question(q_norm: str) -> bool:
    q = normalize_question(q_norm)
    if not q:
        return False
    if "authorized to work" in q and ("us" in q or "united states" in q):
        return True
    if "sponsorship" in q or "h 1b" in q or "visa" in q:
        return True
    return False


def _generic_cv_pitch(profile: Dict[str, str]) -> str:
    parts = _profile_parts(profile)
    summary = (parts.get("summary") or "").strip()
    skills = (parts.get("skills") or "").strip()
    if summary:
        if skills:
            return f"{summary} Core strengths: {skills}."
        return summary
    return "I have 5+ years of QA experience in manual and automation testing, with strong API testing and C#/.NET automation."


def _template_years(q: str) -> str:
    years_map = [
        ("manual testing", "5"),
        ("jira", "5"),
        ("sql", "4"),
        ("postman", "5"),
        ("api", "5"),
        ("rest", "5"),
        ("graphql", "3"),
        ("selenium", "5"),
        ("c#", "5"),
        (".net", "5"),
        ("docker", "3"),
        ("linux", "2"),
        ("azure devops", "2"),
        ("python", "1"),
        ("appium", "0"),
    ]
    for k, v in years_map:
        if k in q:
            return v
    return "0"


def _answer_for_question(
    question: str,
    *,
    field_type: str,
    profile: Dict[str, str],
    db_conn=None,
) -> str:
    q_raw = _clean_question_text(question)
    q = normalize_question(q_raw)
    if not q:
        return ""

    if db_conn is not None:
        got = get_answer(db_conn, q)
        if got and (got[0] or "").strip():
            return str(got[0]).strip()

    if _is_sensitive_question(q):
        return ""

    parts = _profile_parts(profile)
    name = parts["name"]
    first = parts["first"]
    last = parts["last"]
    email = parts["email"]
    phone = parts["phone"]
    phone_local = parts["phone_local"]
    location = parts["location"]
    linkedin = parts["linkedin"]
    github = parts["github"]
    summary = parts["summary"]
    skills = parts["skills"]

    if "first name" in q:
        return first
    if "last name" in q:
        return last
    if "full name" in q or q == "name":
        return name
    if "email" in q:
        return email
    if "phone" in q or "mobile" in q:
        return phone_local or phone
    if "linkedin" in q:
        return linkedin
    if "github" in q:
        return github
    if "city" in q:
        return "Ho Chi Minh City"
    if "location" in q:
        return location
    if "pronouns" in q:
        return ""
    if "years" in q and ("experience" in q or "work experience" in q):
        return _template_years(q)
    if "accommodations" in q:
        return "no"
    if "remote" in q and "comfortable" in q:
        return "yes"
    if "essential functions" in q:
        return "yes"
    if "background check" in q:
        return "yes"
    if "drug test" in q:
        return "yes"
    if "salary" in q or "compensation" in q:
        return ""
    if "notice period" in q or "start date" in q or "when can you start" in q:
        return str(profile.get("candidate.availability") or "Immediate").strip()
    if "country" in q:
        return "Vietnam"
    if "state" in q or "province" in q:
        return "Ho Chi Minh City"
    if "currently employed" in q:
        return "yes"
    if "current employer" in q or "current company" in q:
        return "FCE GROUP AG"
    if "highest degree" in q or "education" in q:
        return "Bachelor's degree"
    if "portfolio" in q or "website" in q:
        return linkedin
    if "cover letter" in q:
        return _generic_cv_pitch(profile)
    if "about yourself" in q or "tell us about your" in q:
        ans = _generic_cv_pitch(profile)
        return ans[:1200]

    if field_type == "radio":
        if "yes" in q and "no" not in q:
            return "yes"
    if field_type in {"text", "textarea"}:
        return _generic_cv_pitch(profile)[:700]
    if field_type == "number":
        if "year" in q or "experience" in q:
            return "5"
        return "1"
    return ""


def _pick_select_value(options: List[str], desired: str) -> str:
    opts = [str(o or "").strip() for o in options if str(o or "").strip()]
    if not opts:
        return ""
    if desired:
        for o in opts:
            if o.lower() == desired.lower():
                return o
        for o in opts:
            if desired.lower() in o.lower():
                return o
    for o in opts:
        low = o.lower().strip()
        if low in {"select", "select...", "choose", "choose...", "n/a", "none"}:
            continue
        if low.startswith("select ") or "please select" in low:
            continue
        if low in {"-", "--", "---"}:
            continue
        return o
    return opts[0]


async def _try_submit_forms(page) -> bool:
    try:
        return bool(
            await page.evaluate(
                """() => {
                  const forms = Array.from(document.querySelectorAll('form'));
                  for (const f of forms) {
                    const r = f.getBoundingClientRect();
                    if (!r || r.width <= 0 || r.height <= 0) continue;
                    try {
                      if (typeof f.reportValidity === 'function' && !f.reportValidity()) continue;
                    } catch (e) {}
                    try {
                      if (typeof f.requestSubmit === 'function') {
                        f.requestSubmit();
                        return true;
                      }
                      const sb = f.querySelector('button[type=submit],input[type=submit]');
                      if (sb) {
                        sb.click();
                        return true;
                      }
                    } catch (e) {}
                  }
                  return false;
                }"""
            )
        )
    except Exception:
        return False


async def _control_meta(el) -> Dict[str, Any]:
    meta = await el.evaluate(
        r"""(el) => {
          const tag = (el.tagName || '').toLowerCase();
          const type = (el.getAttribute('type') || '').toLowerCase();
          const name = (el.getAttribute('name') || '').trim();
          const id = (el.id || '').trim();
          const required = !!el.required || ((el.getAttribute('aria-required') || '').toLowerCase() === 'true');
          const ariaInvalid = ((el.getAttribute('aria-invalid') || '').toLowerCase() === 'true');
          const placeholder = (el.getAttribute('placeholder') || '').trim();
          const ariaLabel = (el.getAttribute('aria-label') || '').trim();
          const accept = (el.getAttribute('accept') || '').trim();
          const checked = !!el.checked || ((el.getAttribute('aria-checked') || '').toLowerCase() === 'true');
          const autocomplete = (el.getAttribute('autocomplete') || '').trim();
          let value = (tag === 'select')
            ? ((el.selectedOptions && el.selectedOptions[0]) ? ((el.selectedOptions[0].innerText || '').trim()) : '')
            : ((el.value || '').trim());
          if (tag === 'input' && (type === 'radio' || type === 'checkbox')) {
            value = checked ? (value || 'true') : '';
          }

          let labelText = '';
          try {
            if (el.labels && el.labels.length) labelText = (el.labels[0].innerText || '').trim();
          } catch (e) {}
          if (!labelText && id) {
            const lab = document.querySelector('label[for=\"' + id + '\"]');
            if (lab) labelText = (lab.innerText || '').trim();
          }

          let legend = '';
          try {
            const fs = el.closest('fieldset');
            if (fs) {
              const lg = fs.querySelector('legend');
              if (lg) legend = (lg.innerText || '').trim();
            }
          } catch (e) {}

          let optionLabel = '';
          try {
            if (tag === 'input' && (type === 'radio' || type === 'checkbox')) {
              if (el.labels && el.labels.length) optionLabel = (el.labels[0].innerText || '').trim();
              if (!optionLabel && id) {
                const lab = document.querySelector('label[for=\"' + id + '\"]');
                if (lab) optionLabel = (lab.innerText || '').trim();
              }
            }
          } catch (e) {}

          let question = (legend || labelText || ariaLabel || placeholder || name || '').replace(/\s+/g,' ').trim();
          let options = [];
          if (tag === 'select') {
            try {
              options = Array.from(el.options || []).map((o) => (o.innerText || '').trim()).filter(Boolean);
            } catch (e) {}
          }
          return {
            tag, type, name, id, required, ariaInvalid, placeholder, ariaLabel,
            accept, checked, autocomplete, value, question, optionLabel, options
          };
        }"""
    )
    return meta if isinstance(meta, dict) else {}


async def _prepare_external_page(page, *, expected_domain: str = ""):
    for btn_name in ["Accept all", "Accept All", "I agree", "Agree", "Allow all"]:
        try:
            b = page.get_by_role("button", name=re.compile(re.escape(btn_name), re.IGNORECASE)).first
            if await b.is_visible(timeout=700):
                await b.click()
                await page.wait_for_timeout(500)
                break
        except Exception:
            continue

    clicked = False
    locators = [
        page.get_by_role("button", name=re.compile(r"apply for this job|apply now|start application|continue application", re.IGNORECASE)),
        page.get_by_role("link", name=re.compile(r"apply for this job|apply now|start application|continue application", re.IGNORECASE)),
        page.get_by_role("tab", name=re.compile(r"application", re.IGNORECASE)),
        page.locator("button:has-text('Apply')"),
        page.locator("a:has-text('Apply')"),
        page.locator("button:has-text('Application')"),
        page.locator("text=/apply for this job|apply now|start application|continue application/i"),
    ]
    for loc in locators:
        if clicked:
            break
        try:
            c = await loc.count()
        except Exception:
            c = 0
        if c <= 0:
            continue
        for i in range(min(c, 3)):
            el = loc.nth(i)
            try:
                await el.scroll_into_view_if_needed(timeout=1200)
            except Exception:
                pass
            try:
                await el.click(timeout=2200)
                await page.wait_for_timeout(900)
                clicked = True
                break
            except Exception:
                try:
                    await el.evaluate("(n) => n.click()")
                    await page.wait_for_timeout(900)
                    clicked = True
                    break
                except Exception:
                    continue

    # Ashby has a stable application route that bypasses flaky CTA click.
    try:
        host = _domain(page.url)
    except Exception:
        host = ""
    if "ashbyhq.com" in host and "/application" not in (page.url or ""):
        try:
            await page.goto((page.url or "").rstrip("/") + "/application", wait_until="domcontentloaded", timeout=20_000)
            await page.wait_for_timeout(900)
        except Exception:
            pass

    # Some job boards open the actual form in a new tab/window.
    try:
        pages = [p for p in page.context.pages if p is not page]
        fallback = None
        for p in reversed(pages):
            url = (p.url or "").strip()
            if not url or not url.lower().startswith("http"):
                continue
            if _is_linkedin_url(url):
                continue
            dom = _domain(url)
            if not dom:
                continue
            if expected_domain and (expected_domain in dom or dom in expected_domain):
                try:
                    await p.bring_to_front()
                except Exception:
                    pass
                return p
            if fallback is None:
                fallback = p
        if fallback is not None:
            try:
                await fallback.bring_to_front()
            except Exception:
                pass
            return fallback
    except Exception:
        pass
    return page


def _profile_parts(profile: Dict[str, str]) -> Dict[str, str]:
    name = str(profile.get("candidate.name") or "").title().strip()
    first, last = _split_name(name)
    phone_raw = str(profile.get("candidate.phone") or "").strip()
    phone = re.sub(r"[^0-9]", "", phone_raw)
    if phone.startswith("84") and len(phone) >= 10:
        phone_local = phone[2:]
    else:
        phone_local = phone
    return {
        "name": name,
        "first": first,
        "last": last,
        "email": str(profile.get("candidate.email") or "").strip(),
        "phone_raw": phone_raw,
        "phone": phone,
        "phone_local": phone_local,
        "linkedin": str(profile.get("candidate.linkedin") or "").strip(),
        "github": str(profile.get("candidate.github") or "").strip(),
        "location": str(profile.get("candidate.location") or "").strip(),
        "summary": str(profile.get("candidate.summary") or "").strip(),
        "skills": str(profile.get("candidate.skills") or "").strip(),
    }


def _answer_for_control(meta: Dict[str, Any], *, profile: Dict[str, str]) -> str:
    parts = _profile_parts(profile)
    tag = str(meta.get("tag") or "").lower()
    typ = str(meta.get("type") or "").lower()
    blob = " ".join(
        [
            str(meta.get("question") or ""),
            str(meta.get("ariaLabel") or ""),
            str(meta.get("placeholder") or ""),
            str(meta.get("name") or ""),
            str(meta.get("id") or ""),
            str(meta.get("autocomplete") or ""),
        ]
    ).lower()
    q_norm = normalize_question(blob)

    if _is_sensitive_question(q_norm):
        return ""

    if typ == "email" or "email" in blob:
        return parts["email"]
    if typ in {"tel", "phone"} or "phone" in blob or "mobile" in blob:
        return parts.get("phone_raw", "") or parts["phone_local"] or parts["phone"]
    if typ == "url":
        if "linkedin" in blob:
            return parts["linkedin"]
        if "github" in blob:
            return parts["github"]
    if "first name" in blob or ("given" in blob and "name" in blob):
        return parts["first"]
    if "last name" in blob or ("family" in blob and "name" in blob) or ("surname" in blob):
        return parts["last"]
    if "full name" in blob or (typ == "text" and "name" in blob):
        return parts["name"]
    if "city" in blob:
        return "Ho Chi Minh City"
    if "location" in blob:
        return parts["location"]
    if "linkedin" in blob:
        return parts["linkedin"]
    if "github" in blob:
        return parts["github"]
    if "cover letter" in blob or ("about" in blob and tag == "textarea"):
        ans = _generic_cv_pitch(profile)
        return ans[:1200]
    if tag == "textarea":
        return _generic_cv_pitch(profile)[:700]
    if tag == "input" and typ == "number":
        return "5"
    return ""


async def _pick_radio_fallback(page, *, name: str) -> bool:
    if not name:
        return False
    radios = page.locator(f"input[type='radio'][name='{name}']")
    rc = await radios.count()
    if rc <= 0:
        return False

    first_visible = None
    yes_candidate = None
    no_candidate = None
    for ri in range(rc):
        r = radios.nth(ri)
        try:
            if not await r.is_visible(timeout=80):
                continue
        except Exception:
            continue
        if first_visible is None:
            first_visible = r
        try:
            rmeta = await _control_meta(r)
            blob = " ".join(
                [
                    str(rmeta.get("optionLabel") or "").lower(),
                    str(rmeta.get("value") or "").lower(),
                    str(rmeta.get("ariaLabel") or "").lower(),
                ]
            )
            if yes_candidate is None and any(x in blob for x in ["yes", "true", "1"]):
                yes_candidate = r
            if no_candidate is None and any(x in blob for x in ["no", "false", "0"]):
                no_candidate = r
        except Exception:
            pass

    target = yes_candidate or first_visible or no_candidate
    if target is None:
        return False
    try:
        await target.check()
        return True
    except Exception:
        try:
            await target.click()
            return True
        except Exception:
            return False


async def _apply_answer_to_control(page, el, meta: Dict[str, Any], answer: str) -> bool:
    ans = str(answer or "").strip()
    if not ans:
        return False

    tag = str(meta.get("tag") or "").lower()
    typ = str(meta.get("type") or "").lower()

    try:
        if tag == "input" and typ in {"text", "email", "tel", "number", "url"}:
            if typ == "tel":
                # Workable phone widgets may ignore plain fill(); fallback to typed input.
                variants = []
                variants.append(ans)
                compact_plus = re.sub(r"[^\d+]", "", ans)
                compact_digits = re.sub(r"\D", "", ans)
                if compact_plus and compact_plus not in variants:
                    variants.append(compact_plus)
                if compact_digits and compact_digits not in variants:
                    variants.append(compact_digits)

                for v in variants:
                    try:
                        await el.click()
                    except Exception:
                        pass
                    try:
                        await el.fill("")
                    except Exception:
                        pass
                    try:
                        await el.type(v, delay=45)
                    except Exception:
                        try:
                            await el.fill(v)
                        except Exception:
                            continue
                    try:
                        cur = (await el.input_value()).strip()
                    except Exception:
                        cur = ""
                    if cur:
                        return True
                return False

            await el.fill(ans)
            return True
        if tag == "textarea":
            await el.fill(ans)
            return True
        if tag == "select":
            try:
                await el.select_option(label=ans)
                return True
            except Exception:
                pick = _pick_select_value(list(meta.get("options") or []), ans)
                if not pick:
                    return False
                await el.select_option(label=pick)
                return True
        if tag == "input" and typ == "checkbox":
            low = ans.lower()
            if low in {"yes", "true", "1"}:
                await el.check()
            else:
                await el.uncheck()
            return True
        if tag == "input" and typ == "radio":
            rn = str(meta.get("name") or "").strip()
            radios = page.locator(f"input[type='radio'][name='{rn}']") if rn else page.locator("input[type='radio']")
            rc = await radios.count()
            target = None
            low_ans = ans.lower()
            for ri in range(rc):
                r = radios.nth(ri)
                try:
                    if not await r.is_visible(timeout=80):
                        continue
                except Exception:
                    continue
                rmeta = await _control_meta(r)
                blob = " ".join(
                    [
                        str(rmeta.get("optionLabel") or "").lower(),
                        str(rmeta.get("value") or "").lower(),
                        str(rmeta.get("ariaLabel") or "").lower(),
                    ]
                ).strip()
                if low_ans and low_ans in blob:
                    target = r
                    break
                if low_ans in {"yes", "true", "1"} and any(x in blob for x in ["yes", "true", "1"]):
                    target = r
                    break
                if low_ans in {"no", "false", "0"} and any(x in blob for x in ["no", "false", "0"]):
                    target = r
                    break
            if target is None:
                target = radios.first if rc > 0 else None
            if target is None:
                return False
            try:
                await target.check()
            except Exception:
                await target.click()
            return True
    except Exception:
        return False

    return False


async def _fill_external_required(
    page,
    *,
    profile: Dict[str, str],
    db_conn,
    resume_path: Path,
    controller: Optional[AutoController] = None,
    job_context: Optional[Dict[str, Any]] = None,
) -> Tuple[bool, List[Dict[str, Any]], bool]:
    """
    Returns:
      all_filled, missing, photo_required_skip
    """
    controls = page.locator("input, textarea, select")
    n = await controls.count()
    missing: List[Dict[str, Any]] = []
    missing_seen: set[str] = set()
    processed_radio_names: set[str] = set()
    controller_candidates: List[Dict[str, Any]] = []
    photo_required = False

    for i in range(n):
        el = controls.nth(i)
        try:
            if not await el.is_visible(timeout=80):
                continue
        except Exception:
            continue
        try:
            meta = await _control_meta(el)
        except Exception:
            continue

        tag = str(meta.get("tag") or "")
        typ = str(meta.get("type") or "")
        if tag == "input" and typ in {"hidden", "submit", "button"}:
            continue
        required = bool(meta.get("required")) or bool(meta.get("ariaInvalid"))
        val = str(meta.get("value") or "").strip()
        q_raw = _clean_question_text(str(meta.get("question") or ""))
        qn = normalize_question(q_raw)

        if tag == "input" and typ == "radio":
            rn = str(meta.get("name") or "").strip()
            if rn and rn in processed_radio_names:
                continue
            if rn:
                processed_radio_names.add(rn)

        if not required:
            continue

        if tag == "input" and typ == "file":
            accept = str(meta.get("accept") or "").lower()
            q_low = q_raw.lower()
            if ("image/" in accept or ".jpg" in accept or ".jpeg" in accept or ".png" in accept or ".gif" in accept) or ("photo" in q_low):
                photo_required = True
                continue
            if resume_path.exists():
                try:
                    await el.set_input_files(str(resume_path))
                    await page.wait_for_timeout(300)
                    continue
                except Exception:
                    pass
            k = qn or q_raw.lower() or f"file_{i}"
            if k not in missing_seen:
                missing_seen.add(k)
                missing.append({"question": q_raw, "q_norm": qn, "tag": tag, "type": typ})
            continue

        if val and typ not in {"radio", "checkbox"}:
            continue

        ans = _answer_for_question(q_raw, field_type=typ, profile=profile, db_conn=db_conn)
        if not ans:
            ans = _answer_for_control(meta, profile=profile)
        if not ans:
            sensitive = _is_sensitive_question(qn)
            if not sensitive and tag == "select":
                pick = _pick_select_value(list(meta.get("options") or []), "")
                if pick:
                    try:
                        await el.select_option(label=pick)
                        continue
                    except Exception:
                        pass
            if not sensitive and tag == "input" and typ == "radio":
                rn = str(meta.get("name") or "").strip()
                if await _pick_radio_fallback(page, name=rn):
                    continue
            if not sensitive and tag == "input" and typ == "checkbox":
                try:
                    await el.check()
                    continue
                except Exception:
                    pass
            if not sensitive and ((tag == "textarea") or (tag == "input" and typ in {"text", "number", "url"})):
                ans = _answer_for_control(meta, profile=profile)
                if ans:
                    try:
                        await el.fill(ans)
                        continue
                    except Exception:
                        pass
            if not sensitive and controller is not None and controller.is_ready():
                controller_candidates.append(
                    {
                        "el": el,
                        "meta": meta,
                        "question": q_raw,
                        "q_norm": qn,
                        "tag": tag,
                        "type": typ,
                        "options": list(meta.get("options") or []),
                    }
                )
                continue
            k = qn or q_raw.lower() or f"q_{i}"
            if k not in missing_seen:
                missing_seen.add(k)
                missing.append({"question": q_raw, "q_norm": qn, "tag": tag, "type": typ, "options": list(meta.get("options") or [])})
            continue

        ok_apply = await _apply_answer_to_control(page, el, meta, ans)
        if ok_apply:
            if db_conn is not None and q_raw and ans:
                try:
                    insert_answer_if_missing(db_conn, q_raw=q_raw, answer=ans, status="draft")
                except Exception:
                    pass
        else:
            k = qn or q_raw.lower() or f"q_{i}"
            if k not in missing_seen:
                missing_seen.add(k)
                missing.append({"question": q_raw, "q_norm": qn, "tag": tag, "type": typ, "options": list(meta.get("options") or [])})

    if controller_candidates and controller is not None and controller.is_ready():
        try:
            page_title = await page.title()
        except Exception:
            page_title = ""
        controller_answers = await controller.suggest_answers(
            page_url=page.url or "",
            page_title=page_title or "",
            questions=[
                {
                    "q_norm": c.get("q_norm") or "",
                    "question": c.get("question") or "",
                    "tag": c.get("tag") or "",
                    "type": c.get("type") or "",
                    "options": c.get("options") or [],
                }
                for c in controller_candidates
            ],
            profile=profile,
            context=job_context or {},
        )
        for c in controller_candidates:
            q_raw = str(c.get("question") or "")
            qn = normalize_question(str(c.get("q_norm") or q_raw))
            ans = str(controller_answers.get(qn) or "").strip()
            if not ans:
                k = qn or q_raw.lower()
                if k and k not in missing_seen:
                    missing_seen.add(k)
                    missing.append(
                        {
                            "question": q_raw,
                            "q_norm": qn,
                            "tag": str(c.get("tag") or ""),
                            "type": str(c.get("type") or ""),
                            "options": list(c.get("options") or []),
                        }
                    )
                continue
            ok_apply = await _apply_answer_to_control(page, c["el"], c["meta"], ans)
            if not ok_apply:
                k = qn or q_raw.lower()
                if k and k not in missing_seen:
                    missing_seen.add(k)
                    missing.append(
                        {
                            "question": q_raw,
                            "q_norm": qn,
                            "tag": str(c.get("tag") or ""),
                            "type": str(c.get("type") or ""),
                            "options": list(c.get("options") or []),
                        }
                    )
                continue
            if db_conn is not None and q_raw:
                try:
                    insert_answer_if_missing(db_conn, q_raw=q_raw, answer=ans, status="draft")
                except Exception:
                    pass

    return (len(missing) == 0 and not photo_required, missing, photo_required)


async def _find_external_primary_button(
    page,
    *,
    controller: Optional[AutoController] = None,
    job_context: Optional[Dict[str, Any]] = None,
):
    names = [r"Submit", r"Submit application", r"Apply", r"Send application", r"Complete application", r"Next", r"Continue"]
    bad = ["cookie", "settings", "share", "save", "cancel", "decline"]
    best = None
    best_score = -10_000
    controller_pool: List[Dict[str, Any]] = []
    candidates = page.locator("button, input[type='submit'], input[type='button'], a[role='button']")
    n = await candidates.count()
    for i in range(n):
        b = candidates.nth(i)
        try:
            meta = await b.evaluate(
                """(el) => {
                  const txt = ((el.innerText || el.textContent || el.value || '') + '').replace(/\\s+/g,' ').trim();
                  const aria = (el.getAttribute('aria-label') || '').trim();
                  const cls = (el.className || '').toString();
                  const tag = (el.tagName || '').toLowerCase();
                  const type = (el.getAttribute('type') || '').toLowerCase();
                  const disabled = !!el.disabled || (el.getAttribute('aria-disabled') || '').toLowerCase() === 'true';
                  const style = window.getComputedStyle(el);
                  const hidden = (style.display === 'none' || style.visibility === 'hidden' || style.opacity === '0');
                  const r = el.getBoundingClientRect();
                  return {txt, aria, cls, tag, type, disabled, hidden, x:r.x||0, y:r.y||0, w:r.width||0, h:r.height||0};
                }"""
            )
        except Exception:
            continue
        label = f"{meta.get('txt','')} {meta.get('aria','')}".strip().lower()
        if any(bd in label for bd in bad):
            continue
        controller_pool.append(
            {
                "locator": b,
                "label": label,
                "tag": str(meta.get("tag") or ""),
                "type": str(meta.get("type") or ""),
                "disabled": bool(meta.get("disabled")),
                "x": float(meta.get("x") or 0),
                "y": float(meta.get("y") or 0),
                "w": float(meta.get("w") or 0),
                "h": float(meta.get("h") or 0),
            }
        )
        score = 0
        for idx, rx in enumerate(names):
            if re.search(rx, label, re.IGNORECASE):
                score += (120 - idx * 12)
                break
        if score <= 0:
            continue
        if bool(meta.get("hidden")):
            score -= 40
        if float(meta.get("w") or 0) <= 0 or float(meta.get("h") or 0) <= 0:
            score -= 40
        if str(meta.get("tag") or "") == "input" and str(meta.get("type") or "") == "submit":
            score += 25
        if float(meta.get("y") or 0) > 260:
            score += 15
        if float(meta.get("x") or 0) > 300:
            score += 8
        if bool(meta.get("disabled")):
            score -= 20
        if score > best_score:
            best_score = score
            best = b
    if best is not None:
        return best

    if controller is not None and controller.is_ready() and controller_pool:
        try:
            page_title = await page.title()
        except Exception:
            page_title = ""
        controller_index = await controller.choose_primary_button(
            page_url=page.url or "",
            page_title=page_title or "",
            buttons=controller_pool,
            context=job_context or {},
        )
        if controller_index is not None:
            try:
                return controller_pool[controller_index]["locator"]
            except Exception:
                pass

    # Fallback by visible text nodes.
    for txt in ["Submit", "Apply", "Continue", "Next"]:
        try:
            loc = page.locator(f"text=/{txt}/i").first
            if await loc.count() > 0:
                return loc
        except Exception:
            continue
    return None


async def _detect_external_submitted(page) -> bool:
    try:
        txt = (await page.evaluate("() => document.body ? document.body.innerText : ''")).lower()
    except Exception:
        txt = ""
    url = (page.url or "").lower()
    positive = [
        "application submitted",
        "application received",
        "thank you for applying",
        "thanks for applying",
        "your application has been submitted",
        "we have received your application",
    ]
    if any(p in txt for p in positive):
        return True
    if any(x in url for x in ["/submitted", "/thank", "/success", "/complete"]):
        return True
    return False


async def _detect_external_blocker(page) -> str:
    try:
        txt = (await page.evaluate("() => document.body ? document.body.innerText : ''")).lower()
    except Exception:
        txt = ""
    if "403 forbidden" in txt:
        return "blocked_403"
    if "verify you are human" in txt or "checking your browser" in txt:
        return "blocked_cloudflare"
    if "captcha" in txt and "cloudflare" in txt:
        return "blocked_cloudflare"
    try:
        html = (
            await page.evaluate(
                "() => document && document.documentElement ? document.documentElement.outerHTML : ''"
            )
        ).lower()
    except Exception:
        html = ""
    if "/cdn-cgi/challenge-platform/" in html:
        return "blocked_cloudflare"
    if "__cf$cv$params" in html:
        return "blocked_cloudflare"
    if "cf-turnstile" in html:
        return "blocked_cloudflare"
    return ""


async def _has_cloudflare_challenge(page) -> bool:
    try:
        txt = (await page.evaluate("() => document.body ? document.body.innerText : ''")).lower()
    except Exception:
        txt = ""
    if "verify you are human" in txt or "checking your browser" in txt or "just a moment" in txt:
        return True
    try:
        html = (
            await page.evaluate(
                "() => document && document.documentElement ? document.documentElement.outerHTML : ''"
            )
        ).lower()
    except Exception:
        html = ""
    return any(
        marker in html
        for marker in (
            "/cdn-cgi/challenge-platform/",
            "__cf$cv$params",
            "cf-turnstile",
        )
    )


async def _wait_cloudflare_challenge(page, *, max_wait_sec: int = 75) -> bool:
    # Some hosts (e.g. Workable) show temporary Cloudflare challenge pages
    # before the real application form appears.
    if not await _has_cloudflare_challenge(page):
        return True
    loops = max(1, int(max_wait_sec // 3))
    for _ in range(loops):
        await page.wait_for_timeout(3000)
        if not await _has_cloudflare_challenge(page):
            return True
    return False


async def _wait_external_surface_ready(page, *, max_wait_sec: int = 25) -> bool:
    """
    Wait until dynamic job UIs (e.g. Workable) render visible content.
    Without this, we can hit a white/skeleton state and miss the primary CTA.
    """
    loops = max(1, int(max_wait_sec))
    for _ in range(loops):
        try:
            ready = await page.evaluate(
                """() => {
                  const body = document.body;
                  if (!body) return false;
                  const txt = (body.innerText || '').replace(/\\s+/g, ' ').trim();
                  const nodes = Array.from(document.querySelectorAll('button,a,[role="button"],[role="link"]'));
                  const hasCta = nodes.some((n) => {
                    const t = ((n.innerText || '') + ' ' + (n.getAttribute('aria-label') || '')).toLowerCase();
                    return /apply|application|submit|continue|next|send/.test(t);
                  });
                  const hasInputs = document.querySelectorAll('input,select,textarea').length > 0;
                  const hasHeading = !!document.querySelector('h1,h2,[role="heading"]');
                  return (txt.length > 80) && (hasCta || hasInputs || hasHeading);
                }"""
            )
        except Exception:
            ready = False
        if bool(ready):
            return True
        await page.wait_for_timeout(1000)
    return False


async def _run_external_apply_once(
    *,
    root: Path,
    page,
    external_url: str,
    profile: Dict[str, str],
    db_conn,
    resume_path: Path,
    max_steps: int,
    controller: Optional[AutoController] = None,
    job_context: Optional[Dict[str, Any]] = None,
) -> Tuple[str, str]:
    try:
        await page.goto(external_url, wait_until="domcontentloaded", timeout=35_000)
    except Exception:
        await dump_debug(root, page, "external_open_failed")
        return ("failed", "external_open_failed")

    await _wait_external_surface_ready(page, max_wait_sec=20)

    if _is_linkedin_url(page.url):
        return ("needs_manual", "redirected_back_to_linkedin")

    page = await _prepare_external_page(page, expected_domain=_domain(external_url))
    await _wait_external_surface_ready(page, max_wait_sec=20)
    if not await _wait_cloudflare_challenge(page, max_wait_sec=75):
        await dump_debug(root, page, "external_blocked_cloudflare_wait_timeout")
        return ("needs_manual", "blocked_cloudflare")

    blocker = await _detect_external_blocker(page)
    if blocker:
        await dump_debug(root, page, "external_blocked")
        return ("needs_manual", blocker)

    for step in range(max_steps):
        if await _has_cloudflare_challenge(page):
            if not await _wait_cloudflare_challenge(page, max_wait_sec=40):
                await dump_debug(root, page, f"external_blocked_cloudflare_s{step+1}")
                return ("needs_manual", "blocked_cloudflare")
        blocker = await _detect_external_blocker(page)
        if blocker:
            await dump_debug(root, page, f"external_blocked_s{step+1}")
            return ("needs_manual", blocker)
        if await _detect_external_submitted(page):
            return ("submitted", "external_detected_submitted")

        ok_fill, missing, photo_required = await _fill_external_required(
            page,
            profile=profile,
            db_conn=db_conn,
            resume_path=resume_path,
            controller=controller,
            job_context=job_context,
        )
        if photo_required:
            await dump_debug(root, page, f"external_photo_required_s{step+1}")
            return ("needs_manual", "photo_required_skip")

        if not ok_fill:
            await dump_debug(root, page, f"external_missing_required_s{step+1}")
            return (
                "needs_manual",
                json.dumps(
                    {"reason": "missing_required_questions", "step": step + 1, "missing": missing},
                    ensure_ascii=False,
                ),
            )

        btn = await _find_external_primary_button(page, controller=controller, job_context=job_context)
        if btn is None:
            if await _try_submit_forms(page):
                await page.wait_for_timeout(random.randint(1000, 1800))
                continue
            await dump_debug(root, page, f"external_no_primary_button_s{step+1}")
            if await _detect_external_submitted(page):
                return ("submitted", "external_detected_submitted_no_button")
            return ("needs_manual", "no_external_primary_button")

        try:
            if not await btn.is_enabled():
                await dump_debug(root, page, f"external_button_disabled_s{step+1}")
                return ("needs_manual", "external_primary_button_disabled")
        except Exception:
            pass

        try:
            await btn.scroll_into_view_if_needed()
        except Exception:
            pass
        clicked = False
        for _ in range(2):
            try:
                await btn.click(timeout=4000)
                clicked = True
                break
            except Exception:
                await page.wait_for_timeout(500)
                btn = await _find_external_primary_button(page, controller=controller, job_context=job_context)
                if btn is None:
                    break
        if not clicked:
            try:
                if btn is not None:
                    await btn.evaluate("(el) => el.click()")
                    clicked = True
            except Exception:
                pass
        if not clicked:
            await dump_debug(root, page, f"external_click_failed_s{step+1}")
            return ("needs_manual", "external_primary_click_failed")

        await page.wait_for_timeout(random.randint(1000, 1800))

    await dump_debug(root, page, "external_max_steps")
    return ("needs_manual", "external_max_steps")


def _fetch_external_jobs(
    conn,
    *,
    platform: str,
    limit: int,
    include_attempted: bool,
    require_qa_title: bool,
) -> List[Dict[str, str]]:
    where = [
        "l.platform = ?",
        "l.lead_type = 'job'",
        "l.raw_json LIKE '%\"apply_type\": \"external\"%'",
        "NOT EXISTS(SELECT 1 FROM events e WHERE e.lead_id=l.lead_id AND e.event_type='external_apply_submitted')",
    ]
    if not include_attempted:
        where.extend(
            [
                "NOT EXISTS(SELECT 1 FROM events e WHERE e.lead_id=l.lead_id AND e.event_type='external_apply_needs_manual')",
                "NOT EXISTS(SELECT 1 FROM events e WHERE e.lead_id=l.lead_id AND e.event_type='external_apply_failed')",
            ]
        )
    sql = (
        "SELECT l.lead_id, l.contact, l.company, l.job_title, l.raw_json "
        "FROM leads l "
        f"WHERE {' AND '.join(where)} "
        "ORDER BY l.created_at DESC "
        "LIMIT ?"
    )
    rows = conn.execute(sql, (str(platform or "").strip(), int(limit))).fetchall()
    out: List[Dict[str, str]] = []
    for r in rows:
        title = str(r["job_title"] or "")
        if require_qa_title and (not QA_TITLE_RE.search(title)):
            continue
        raw = _parse_json(r["raw_json"])
        ext = str(raw.get("apply_url") or "").strip()
        if not ext or _is_linkedin_url(ext):
            continue
        out.append(
            {
                "lead_id": str(r["lead_id"]),
                "job_url": str(r["contact"] or ""),
                "external_url": ext,
                "company": str(r["company"] or ""),
                "title": title,
            }
        )
    return out


async def run(args: argparse.Namespace) -> int:
    load_env_file(ROOT / ".env")
    load_env_file(ROOT / ".env.accounts")

    cfg_path = resolve_path(ROOT, args.config)
    cfg = load_config(str(cfg_path)) if cfg_path.exists() else {}
    controller = AutoController.from_config(cfg)
    if args.controller:
        controller.enabled = True
    if args.no_controller:
        controller.enabled = False
    if args.controller_model.strip():
        controller.model = args.controller_model.strip()

    db_path = resolve_path(ROOT, str(cfg_get(cfg, "activity.db_path", "data/out/activity.sqlite")))
    if args.db.strip():
        db_path = resolve_path(ROOT, args.db.strip())

    resume_path = resolve_path(ROOT, args.resume)
    if not args.resume.strip():
        attach = cfg_get(cfg, "email.attachments", [])
        if isinstance(attach, list) and attach:
            resume_path = resolve_path(ROOT, str(attach[0]))

    closer = SafeCloser()
    db_conn = None
    try:
        db_conn = db_connect(db_path)
        init_db(db_conn)
        profile = load_profile(db_conn)

        jobs = _fetch_external_jobs(
            db_conn,
            platform=args.platform,
            limit=args.limit,
            include_attempted=args.include_attempted,
            require_qa_title=bool(args.require_qa_title),
        )
        if not jobs:
            print("[ext-apply] no eligible external jobs.")
            return 0

        print(f"[ext-apply] jobs to process: {len(jobs)}")
        if controller.enabled:
            if controller.is_ready():
                print(f"[ext-apply] controller enabled: provider={controller.provider} model={controller.model}")
            else:
                if controller.provider == "openclaw":
                    missing_hint = (
                        f"missing gateway token env {controller.openclaw_gateway_token_env} "
                        "(or ~/.openclaw/openclaw.json token)"
                    )
                else:
                    missing_hint = f"missing env {controller.api_key_env}"
                print(
                    f"[ext-apply] controller configured but not ready ({missing_hint}); "
                    "using deterministic mode"
                )

        closer.pw = await async_playwright().start()
        closer.ctx = await closer.pw.chromium.launch_persistent_context(
            user_data_dir=str(Path(os.getenv("PLAYWRIGHT_USER_DATA_DIR") or (ROOT / "data" / "profiles" / "default"))),
            headless=args.headless,
            slow_mo=args.slow_mo_ms,
            viewport=None,
            locale="en-US",
            extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
            args=["--lang=en-US"],
        )

        submitted = 0
        manual = 0
        failed = 0

        for i, job in enumerate(jobs, start=1):
            lead_id = job["lead_id"]
            ext_url = job["external_url"]
            print(f"[ext-apply] {i}/{len(jobs)} -> {job.get('company','')} | {job.get('title','')} | {ext_url}")
            add_event(
                db_conn,
                lead_id=lead_id,
                event_type="external_apply_started",
                occurred_at=_now_iso(),
                details={"external_url": ext_url, "job_url": job.get("job_url", "")},
            )
            db_conn.commit()

            page = await closer.ctx.new_page()
            page.set_default_timeout(args.step_timeout_ms)
            page.set_default_navigation_timeout(args.step_timeout_ms)
            try:
                result, details = await _run_external_apply_once(
                    root=ROOT,
                    page=page,
                    external_url=ext_url,
                    profile=profile,
                    db_conn=db_conn,
                    resume_path=resume_path,
                    max_steps=args.max_steps,
                    controller=controller,
                    job_context=job,
                )
            except PlaywrightTimeoutError:
                await dump_debug(ROOT, page, "external_timeout")
                result, details = ("failed", "playwright_timeout")
            except Exception as e:
                await dump_debug(ROOT, page, "external_exception")
                result, details = ("failed", f"exception:{type(e).__name__}")
            finally:
                try:
                    await page.close()
                except Exception:
                    pass

            ev = (
                "external_apply_submitted"
                if result == "submitted"
                else ("external_apply_needs_manual" if result == "needs_manual" else "external_apply_failed")
            )
            add_event(
                db_conn,
                lead_id=lead_id,
                event_type=ev,
                occurred_at=_now_iso(),
                details={"details": details, "external_url": ext_url, "job_url": job.get("job_url", "")},
            )
            db_conn.commit()

            if result == "submitted":
                submitted += 1
                notify(ROOT, cfg, kind="done")
            elif result == "needs_manual":
                manual += 1
                notify(ROOT, cfg, kind="attention")
                print(f"[ext-apply] needs_manual: {details}")
            else:
                failed += 1
                notify(ROOT, cfg, kind="error")
                print(f"[ext-apply] failed: {details}")

            await asyncio.sleep(random.uniform(args.min_delay_sec, args.max_delay_sec))

        print(f"[ext-apply] done: submitted={submitted} manual={manual} failed={failed}")
        return 0
    except Exception as e:
        print(f"[ext-apply] error: {e}")
        return 1
    finally:
        try:
            if db_conn is not None:
                db_conn.close()
        except Exception:
            pass
        await closer.close()


def main() -> int:
    ap = argparse.ArgumentParser(description="Submit external apply forms from leads DB (no LinkedIn required).")
    ap.add_argument("--config", default="config/config.yaml")
    ap.add_argument("--db", default="")
    ap.add_argument("--platform", default="linkedin", help="Leads platform to pull from (default: linkedin).")
    ap.add_argument("--limit", type=int, default=10)
    ap.add_argument("--require-qa-title", action="store_true", help="Skip non-QA titles")
    ap.add_argument("--include-attempted", action="store_true")
    ap.add_argument("--resume", default="Docs/ARTEM_BONDARENKO_CV_2026.pdf")
    ap.add_argument("--max-steps", type=int, default=6)
    ap.add_argument("--step-timeout-ms", type=int, default=30000)
    ap.add_argument("--headless", action="store_true")
    ap.add_argument("--slow-mo-ms", type=int, default=0)
    ap.add_argument("--min-delay-sec", type=float, default=1.2)
    ap.add_argument("--max-delay-sec", type=float, default=2.4)
    ap.add_argument("--controller", action="store_true", help="Force-enable controller assist for unknown fields/buttons")
    ap.add_argument("--no-controller", action="store_true", help="Force-disable controller assist")
    ap.add_argument("--controller-model", default="", help="Override controller model name for this run")
    ap.add_argument("--timeout-seconds", type=int, default=3600)
    args = ap.parse_args()

    try:
        return asyncio.run(asyncio.wait_for(run(args), timeout=args.timeout_seconds))
    except asyncio.TimeoutError:
        print("[ext-apply] hard timeout hit; exiting.")
        return 6


if __name__ == "__main__":
    raise SystemExit(main())

