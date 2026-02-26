import asyncio
import json
import random
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .config import cfg_get
from .linkedin_playwright import dump_debug, goto_guarded, is_checkpoint_url
from .profile_store import get_answer, normalize_question


@dataclass
class Candidate:
    first_name: str
    last_name: str
    phone_country: str
    phone_number: str
    email: str


def _split_name(full_name: str) -> Tuple[str, str]:
    parts = [p for p in (full_name or "").strip().split() if p]
    if not parts:
        return ("", "")
    if len(parts) == 1:
        return (parts[0], "")
    return (parts[0], " ".join(parts[1:]))


def candidate_from_cfg(cfg: Dict[str, object], *, profile: Optional[Dict[str, str]] = None) -> Candidate:
    """
    Build Candidate values from (DB profile_kv first) then config.yaml.
    """
    profile = profile or {}
    name = str(profile.get("candidate.name") or cfg_get(cfg, "candidate.name", "")).strip()
    first, last = _split_name(name)
    phone = str(profile.get("candidate.phone") or cfg_get(cfg, "candidate.phone", "")).strip()
    # The UI splits country code and number; we keep it simple and default to Vietnam +84.
    phone_country = "Vietnam (+84)"
    # Strip common separators.
    phone_number = re.sub(r"[^0-9]", "", phone)
    # For +84 phones, LinkedIn expects the local digits without +84. If config includes +84, trim it.
    if phone_number.startswith("84") and len(phone_number) >= 10:
        phone_number = phone_number[2:]
    email = str(profile.get("candidate.email") or cfg_get(cfg, "candidate.email", "")).strip()
    return Candidate(
        first_name=first,
        last_name=last,
        phone_country=phone_country,
        phone_number=phone_number,
        email=email,
    )


async def _fill_if_present(scope, label: str, value: str) -> bool:
    if not value:
        return False
    try:
        loc = scope.get_by_label(label, exact=False).first
        if not await loc.is_visible(timeout=1500):
            return False
        await loc.fill(value)
        return True
    except Exception:
        return False


async def _select_phone_country_if_present(scope, value: str) -> bool:
    if not value:
        return False
    try:
        sel = scope.get_by_label("Phone country code", exact=False).first
        if not await sel.is_visible(timeout=1500):
            return False
        # It can be <select> or a combobox. Try select_option first.
        try:
            await sel.select_option(label=value)
            return True
        except Exception:
            pass
        # Fallback: click and pick from listbox options.
        try:
            await sel.click()
            opt = scope.get_by_role("option", name=re.compile(re.escape(value), re.IGNORECASE)).first
            if await opt.is_visible(timeout=1500):
                await opt.click()
                return True
        except Exception:
            pass
        return False
    except Exception:
        return False


async def _attach_resume_if_possible(scope, resume_path: Path) -> bool:
    if not resume_path.exists():
        return False

    # If a resume option is already visible in the UI, prefer the existing one
    # instead of re-uploading on each step.
    try:
        scope_text = (await _scope_text(scope)).lower()
        stem = resume_path.stem.lower()
        if "resume" in scope_text and (stem in scope_text or ".pdf" in scope_text or ".docx" in scope_text):
            return False
    except Exception:
        pass

    # LinkedIn often uses hidden <input type="file"> for resume/cover letter.
    try:
        inputs = scope.locator("input[type='file']")
        n = await inputs.count()
        if n <= 0:
            return False

        # Prefer the input whose associated label/container mentions "resume".
        # Never upload PDF to photo/image-only controls.
        best_idx = 0
        best_score = -10_000
        for i in range(n):
            inp = inputs.nth(i)
            try:
                meta = await inp.evaluate(
                    """(el) => {
                      const id = el.id || '';
                      const name = el.getAttribute('name') || '';
                      const aria = el.getAttribute('aria-label') || '';
                      const accept = el.getAttribute('accept') || '';
                      const required = !!el.required || ((el.getAttribute('aria-required') || '').toLowerCase() === 'true');
                      const lab = id ? document.querySelector('label[for=\"' + id + '\"]') : null;
                      const labelText = lab ? (lab.innerText || '').trim() : '';
                      const box = el.closest('section,div') || el.parentElement;
                      const boxText = box ? (box.innerText || '').replace(/\\s+/g,' ').trim().slice(0,200) : '';
                      return {id,name,aria,accept,required,labelText,boxText};
                    }"""
                )
                hay = " ".join(
                    [
                        str(meta.get("id") or ""),
                        str(meta.get("name") or ""),
                        str(meta.get("aria") or ""),
                        str(meta.get("accept") or ""),
                        str(meta.get("labelText") or ""),
                        str(meta.get("boxText") or ""),
                    ]
                ).lower()
                score = 0
                if "resume" in hay:
                    score += 3
                if "cv" in hay:
                    score += 2
                if "cover" in hay:
                    score -= 2
                if "photo" in hay or "avatar" in hay or "profile picture" in hay or "image" in hay:
                    score -= 7
                if "image/" in hay or ".jpg" in hay or ".jpeg" in hay or ".png" in hay or ".gif" in hay:
                    score -= 6
                if bool(meta.get("required")) and ("resume" in hay or "cv" in hay):
                    score += 1
                if score > best_score:
                    best_score = score
                    best_idx = i
            except Exception:
                continue

        # If no resume-like control was found, do nothing.
        if best_score <= 0:
            return False

        await inputs.nth(best_idx).set_input_files(str(resume_path))
        return True
    except Exception:
        return False


async def _has_required_photo_upload(scope) -> bool:
    """
    Detect flows requiring a photo/image upload. Per policy, skip these.
    """
    try:
        txt = (await _scope_text(scope)).lower()
    except Exception:
        txt = ""

    if (
        "photo *" in txt
        or "photo*" in txt
        or "upload photo" in txt
        or "profile photo" in txt
        or "acceptable document format (jpg" in txt
        or ("jpg" in txt and "jpeg" in txt and "png" in txt and "gif" in txt and "photo" in txt)
    ):
        return True

    try:
        inputs = scope.locator("input[type='file']")
        n = await inputs.count()
    except Exception:
        n = 0
    for i in range(n):
        inp = inputs.nth(i)
        try:
            if not await inp.is_visible(timeout=80):
                continue
        except Exception:
            continue
        try:
            meta = await inp.evaluate(
                """(el) => {
                  const accept = (el.getAttribute('accept') || '').toLowerCase();
                  const required = !!el.required || ((el.getAttribute('aria-required') || '').toLowerCase() === 'true');
                  const id = el.id || '';
                  const lab = id ? document.querySelector('label[for=\"' + id + '\"]') : null;
                  const labelText = (lab ? (lab.innerText || '') : '').toLowerCase();
                  const box = el.closest('section,div') || el.parentElement;
                  const boxText = (box ? (box.innerText || '') : '').toLowerCase();
                  return {accept, required, labelText, boxText};
                }"""
            )
        except Exception:
            continue
        accept = str(meta.get("accept") or "")
        label_text = str(meta.get("labelText") or "")
        box_text = str(meta.get("boxText") or "")
        hay = f"{accept} {label_text} {box_text}"
        if ("image/" in accept or ".jpg" in accept or ".jpeg" in accept or ".png" in accept or ".gif" in accept) and (
            "photo" in hay or "image" in hay
        ):
            return True
    return False


_IGNORE_Q_NORMS = {
    normalize_question("First name"),
    normalize_question("Last name"),
    normalize_question("Phone country code"),
    normalize_question("Mobile phone number"),
    normalize_question("Email address"),
}


def _is_sensitive_question(q_norm: str) -> bool:
    q = normalize_question(q_norm)
    if not q:
        return False
    # Per user policy: do not auto-answer these unless the user provided an explicit answer in the DB.
    if "authorized to work" in q and "us" in q:
        return True
    if "work in the us" in q and "authorized" in q:
        return True
    if "us business hours" in q:
        return True
    return False


def _template_answer(q_norm: str, *, profile: Optional[Dict[str, str]] = None) -> str:
    """
    Safe, CV-based templates for common questions.
    Unknown questions should be handled by pausing (needs_manual).
    """
    q = normalize_question(q_norm)
    if not q:
        return ""

    # Never guess WordPress-specific experience (often asked in agency roles).
    if "wordpress" in q:
        return ""

    # Remote/distributed team experience.
    if ("worked remotely" in q) or ("distributed team" in q) or ("remote" in q and "team" in q):
        return (
            "Yes. I have 5+ years of experience working in remote/distributed teams. "
            "Most recently I worked remotely as the sole QA owner for a web marketplace + admin portal, "
            "coordinating via Jira and CI pipelines and verifying releases end-to-end."
        )

    # QA interests/tools.
    if ("tools" in q or "technologies" in q) and ("qa" in q or "quality assurance" in q) and ("interesting" in q or "exciting" in q):
        return (
            "API testing (REST/GraphQL) and auth/security testing, C#/.NET automation (NUnit/RestSharp), "
            "CI/CD pipelines (Bitbucket Pipelines, Jenkins, GitLab CI, Docker), and pragmatic UI checks "
            "with Playwright/Selenium for critical flows. I also enjoy release verification and evidence-based defect reporting."
        )

    # Web technologies (keep honest: QA for web/apps/APIs, not a web developer claim).
    if ("web development" in q and "technologies" in q) or ("web technologies" in q):
        return (
            "I mainly test web platforms and APIs. Technologies/tools I work with include REST and GraphQL APIs, "
            "Postman, CI/CD (Bitbucket Pipelines/Jenkins/GitLab CI), Docker, and basic SQL. "
            "I have fundamentals in HTML/CSS and focus on QA for web apps rather than building full web features."
        )

    # Common direct profile fields.
    if "linkedin profile url" in q or ("linkedin" in q and "url" in q):
        return str(profile.get("candidate.linkedin") or "").strip()
    if q == "linkedin":
        return str(profile.get("candidate.linkedin") or "").strip()

    # Years-of-experience questions: safe CV-based mapping for recurring QA tools.
    if "how many years" in q:
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
            ("wordpress", ""),
        ]
        for key, value in years_map:
            if key in q:
                return value
        return "0"

    return ""


def _clean_question_text(text: str) -> str:
    t = str(text or "").replace("\r", "\n")
    lines = [ln.strip() for ln in t.split("\n")]
    lines = [ln for ln in lines if ln and ln.lower() not in {"required"}]
    if not lines:
        return ""
    dedup_lines: List[str] = []
    for ln in lines:
        if not dedup_lines or dedup_lines[-1].lower() != ln.lower():
            dedup_lines.append(ln)
    t = " ".join(dedup_lines)
    t = re.sub(r"\s+", " ", t).strip()
    words = t.split()
    if len(words) >= 8 and len(words) % 2 == 0:
        half = len(words) // 2
        if [w.lower() for w in words[:half]] == [w.lower() for w in words[half:]]:
            t = " ".join(words[:half])
    return t.strip(" :|-")


def _radio_answer_for_question(q_norm: str) -> str:
    q = normalize_question(q_norm)
    if not q:
        return ""
    if _is_sensitive_question(q):
        return ""
    if "authorized to work" in q and "united states" in q:
        return ""
    if "sponsorship" in q or "h 1b" in q:
        return ""
    if "background check" in q:
        return "yes"
    if "drug test" in q:
        return "yes"
    if "perform all of the essential functions" in q:
        return "yes"
    if "comfortable working in a remote setting" in q:
        return "yes"
    if "comfortable working in an onsite setting" in q:
        return "no"
    if "comfortable working in a hybrid setting" in q:
        return "no"
    if "comfortable commuting" in q:
        return "no"
    return ""


async def _control_meta(el) -> Dict[str, Any]:
    """
    Extract best-effort metadata for a form control inside the Easy Apply modal.
    """
    meta = await el.evaluate(
        r"""(el) => {
          const tag = (el.tagName || '').toLowerCase();
          const type = (el.getAttribute('type') || '').toLowerCase();
          const name = (el.getAttribute('name') || '').trim();
          const rawValue = (el.getAttribute('value') || '').trim();
          const required = !!el.required || ((el.getAttribute('aria-required') || '').toLowerCase() === 'true');
          const ariaInvalid = ((el.getAttribute('aria-invalid') || '').toLowerCase() === 'true');
          const ariaLabel = (el.getAttribute('aria-label') || '').trim();
          const placeholder = (el.getAttribute('placeholder') || '').trim();
          const checked = !!el.checked || ((el.getAttribute('aria-checked') || '').toLowerCase() === 'true');
          let value = (tag === 'select')
            ? ((el.selectedOptions && el.selectedOptions[0]) ? ((el.selectedOptions[0].innerText || '').trim()) : '')
            : ((el.value || '').trim());
          if (tag === 'input' && (type === 'radio' || type === 'checkbox')) {
            value = checked ? (value || 'true') : '';
          }

          let labelText = '';
          try {
            if (el.labels && el.labels.length) {
              labelText = (el.labels[0].innerText || '').trim();
            }
          } catch (e) {}

          let labelledBy = '';
          try {
            const ids = (el.getAttribute('aria-labelledby') || '').trim().split(/\s+/).filter(Boolean);
            if (ids.length) {
              labelledBy = ids.map((id) => {
                const n = document.getElementById(id);
                return n ? ((n.innerText || '').trim()) : '';
              }).filter(Boolean).join(' ');
            }
          } catch (e) {}

          // Container heuristic: first visible line of the nearest container text.
          let questionFromContainer = '';
          try {
            const box = el.closest('div') || el.closest('section') || el.parentElement;
            if (box) {
              const lines = (box.innerText || '')
                .split(/\n+/)
                .map((l) => (l || '').trim())
                .filter(Boolean)
                .filter((l) => !/^please enter a valid answer/i.test(l));
              questionFromContainer = (lines[0] || '').trim();
            }
          } catch (e) {}

          let radioGroupQuestion = '';
          try {
            if (tag === 'input' && (type === 'radio' || type === 'checkbox')) {
              const fs = el.closest('fieldset');
              if (fs) {
                const lg = fs.querySelector('legend');
                if (lg) {
                  radioGroupQuestion = (lg.innerText || '').trim();
                }
                if (!radioGroupQuestion) {
                  const anyPrompt = fs.querySelector('h3,h4,p,strong,label,span');
                  if (anyPrompt) {
                    radioGroupQuestion = (anyPrompt.innerText || '').trim();
                  }
                }
              }
            }
          } catch (e) {}

          let optionLabel = '';
          try {
            if (tag === 'input' && (type === 'radio' || type === 'checkbox')) {
              if (el.labels && el.labels.length) {
                optionLabel = (el.labels[0].innerText || '').trim();
              }
              if (!optionLabel) {
                const id = el.id || '';
                const lab = id ? document.querySelector('label[for=\"' + id + '\"]') : null;
                if (lab) {
                  optionLabel = (lab.innerText || '').trim();
                }
              }
            }
          } catch (e) {}

          const question = (radioGroupQuestion || labelText || labelledBy || ariaLabel || questionFromContainer || placeholder || '').trim();

          let options = [];
          if (tag === 'select') {
            try {
              options = Array.from(el.options || []).map((o) => (o.innerText || '').trim()).filter(Boolean);
            } catch (e) {}
          }

          return {
            tag, type, name, rawValue, required, ariaInvalid, ariaLabel, placeholder,
            value, checked, optionLabel, question, options
          };
        }"""
    )
    return meta if isinstance(meta, dict) else {}


async def _extract_required_questions(scope) -> List[Dict[str, Any]]:
    """
    Return a best-effort list of required-but-empty (or invalid) questions.
    Used for debug + manual pause.
    """
    out: List[Dict[str, Any]] = []
    seen: set[str] = set()
    controls = scope.locator("input, textarea, select")
    n = await controls.count()
    seen_radio_names: set[str] = set()
    for i in range(n):
        el = controls.nth(i)
        try:
            if not await el.is_visible(timeout=50):
                continue
        except Exception:
            continue

        try:
            meta = await _control_meta(el)
        except Exception:
            continue

        tag = str(meta.get("tag") or "")
        typ = str(meta.get("type") or "")
        if tag == "input" and typ in {"hidden", "file", "submit", "button"}:
            continue

        q_raw = _clean_question_text(str(meta.get("question") or "").rstrip("*").strip())
        if tag == "input" and typ == "radio":
            rn = str(meta.get("name") or "").strip()
            if rn:
                if rn in seen_radio_names:
                    continue
                seen_radio_names.add(rn)
        qn = normalize_question(q_raw)
        key = qn or q_raw.lower()
        if key in seen:
            continue
        if qn in _IGNORE_Q_NORMS:
            continue

        required = bool(meta.get("required")) or bool(meta.get("ariaInvalid"))
        val = str(meta.get("value") or "").strip()
        if required and not val:
            seen.add(key)
            out.append(
                {
                    "question": q_raw,
                    "q_norm": qn,
                    "tag": tag,
                    "type": typ,
                    "options": meta.get("options") or [],
                }
            )
    return out


async def _fill_additional_questions(
    scope,
    *,
    db_conn=None,
    profile: Optional[Dict[str, str]] = None,
) -> Tuple[bool, List[Dict[str, Any]]]:
    """
    Try to fill required questions using:
    1) answer_bank exact match by normalized question
    2) safe CV-based templates

    Returns (all_filled, missing_questions).
    """
    profile = profile or {}
    missing: List[Dict[str, Any]] = []
    missing_seen: set[str] = set()

    controls = scope.locator("input, textarea, select")
    n = await controls.count()
    seen_radio_names: set[str] = set()
    for i in range(n):
        el = controls.nth(i)
        try:
            if not await el.is_visible(timeout=50):
                continue
        except Exception:
            continue

        try:
            meta = await _control_meta(el)
        except Exception:
            continue

        tag = str(meta.get("tag") or "")
        typ = str(meta.get("type") or "")
        if tag == "input" and typ in {"hidden", "file", "submit", "button"}:
            continue

        q_raw = _clean_question_text(str(meta.get("question") or "").rstrip("*").strip())
        if tag == "input" and typ == "radio":
            rn = str(meta.get("name") or "").strip()
            if rn:
                if rn in seen_radio_names:
                    continue
                seen_radio_names.add(rn)
        qn = normalize_question(q_raw)
        mkey = qn or q_raw.lower()
        if qn in _IGNORE_Q_NORMS:
            continue

        required = bool(meta.get("required")) or bool(meta.get("ariaInvalid"))
        val = str(meta.get("value") or "").strip()
        if not (required and not val):
            continue

        # Per policy: do not auto-answer sensitive questions unless explicitly present in DB.
        ans = ""
        if db_conn is not None and qn:
            got = get_answer(db_conn, qn)
            if got:
                ans = (got[0] or "").strip()

        if not ans:
            if _is_sensitive_question(qn):
                if mkey not in missing_seen:
                    missing_seen.add(mkey)
                    missing.append({"question": q_raw, "q_norm": qn, "tag": tag, "type": typ, "options": meta.get("options") or []})
                continue
            ans = _template_answer(qn, profile=profile)
            if not ans and tag == "input" and typ == "radio":
                ans = _radio_answer_for_question(qn)

        if not ans:
            if mkey not in missing_seen:
                missing_seen.add(mkey)
                missing.append({"question": q_raw, "q_norm": qn, "tag": tag, "type": typ, "options": meta.get("options") or []})
            continue

        # Fill based on control type.
        try:
            if tag == "select":
                try:
                    await el.select_option(label=ans)
                except Exception:
                    # Fallback: best-effort match by option text.
                    opts = [str(o or "").strip() for o in (meta.get("options") or [])]
                    pick = ""
                    for o in opts:
                        if o.lower() == ans.lower():
                            pick = o
                            break
                    if not pick:
                        for o in opts:
                            if ans.lower() in o.lower():
                                pick = o
                                break
                    if pick:
                        await el.select_option(label=pick)
                    else:
                        raise
            elif tag == "input" and typ in {"radio", "checkbox"}:
                low = ans.strip().lower()
                if low not in {"yes", "true", "1", "no", "false", "0"}:
                    raise ValueError("unsupported_radio_checkbox_answer")
                desired_yes = low in {"yes", "true", "1"}

                if typ == "checkbox":
                    if desired_yes:
                        await el.check()
                    else:
                        await el.uncheck()
                else:
                    rn = str(meta.get("name") or "").strip()
                    radios = scope.locator(f"input[type='radio'][name='{rn}']") if rn else scope.locator("input[type='radio']")
                    rc = await radios.count()
                    picked = False
                    for ri in range(rc):
                        r = radios.nth(ri)
                        try:
                            if not await r.is_visible(timeout=50):
                                continue
                        except Exception:
                            continue
                        try:
                            rmeta = await _control_meta(r)
                        except Exception:
                            continue
                        ol = str(rmeta.get("optionLabel") or "").lower()
                        rv = str(rmeta.get("rawValue") or "").lower()
                        al = str(rmeta.get("ariaLabel") or "").lower()
                        blob = f"{ol} {rv} {al}".strip()
                        if desired_yes and ("yes" in blob or rv in {"true", "1"}):
                            await r.check()
                            picked = True
                            break
                        if (not desired_yes) and ("no" in blob or rv in {"false", "0"}):
                            await r.check()
                            picked = True
                            break
                    if not picked:
                        raise ValueError("radio_option_not_found")
            else:
                await el.fill(ans)
        except Exception:
            if mkey not in missing_seen:
                missing_seen.add(mkey)
                missing.append({"question": q_raw, "q_norm": qn, "tag": tag, "type": typ, "options": meta.get("options") or []})

    return (len(missing) == 0, missing)


async def extract_filled_answers(scope) -> List[Dict[str, str]]:
    """
    Extract (question, answer) pairs from visible form controls.
    Used after the user fills answers manually so we can learn them.
    """
    out: List[Dict[str, str]] = []
    controls = scope.locator("input, textarea, select")
    n = await controls.count()
    for i in range(n):
        el = controls.nth(i)
        try:
            if not await el.is_visible(timeout=50):
                continue
        except Exception:
            continue
        try:
            meta = await _control_meta(el)
        except Exception:
            continue

        tag = str(meta.get("tag") or "")
        typ = str(meta.get("type") or "")
        if tag == "input" and typ in {"hidden", "file", "submit", "button"}:
            continue

        q_raw = str(meta.get("question") or "").strip().rstrip("*").strip()
        qn = normalize_question(q_raw)
        if not q_raw or qn in _IGNORE_Q_NORMS:
            continue

        val = str(meta.get("value") or "").strip()
        if not val:
            continue

        out.append({"question": q_raw, "q_norm": qn, "answer": val})
    return out


async def _find_primary_button(scope):
    """
    Find the modal primary action button (Next/Review/Submit) robustly.
    LinkedIn pages often have background "Next" buttons (carousels), so we score
    all visible buttons in scope and pick the best footer-like action button.
    """
    preferred = [
        "submit application",
        "submit",
        "review",
        "next",
        "continue",
        "done",
    ]
    re_primary = re.compile(r"(submit application|submit|review|next|continue|done)", re.IGNORECASE)
    bad_tokens = {"carousel", "pager", "pagination", "slide", "swiper", "slick"}

    best_idx = -1
    best_score = -10_000
    buttons = scope.locator("button")
    try:
        n = await buttons.count()
    except Exception:
        n = 0

    for i in range(n):
        btn = buttons.nth(i)
        try:
            if not await btn.is_visible(timeout=80):
                continue
        except Exception:
            continue

        try:
            meta = await btn.evaluate(
                """(el) => {
                  const txt = (el.innerText || el.textContent || '').replace(/\\s+/g, ' ').trim();
                  const aria = (el.getAttribute('aria-label') || '').trim();
                  const testid = (el.getAttribute('data-testid') || '').trim();
                  const cls = (el.className || '').toString();
                  const disabled = !!el.disabled || (el.getAttribute('aria-disabled') || '').toLowerCase() === 'true';
                  const r = el.getBoundingClientRect();
                  return {
                    txt, aria, testid, cls, disabled,
                    x: r.x || 0, y: r.y || 0, w: r.width || 0, h: r.height || 0
                  };
                }"""
            )
        except Exception:
            continue

        text = str(meta.get("txt") or "").strip()
        aria = str(meta.get("aria") or "").strip()
        testid = str(meta.get("testid") or "").strip().lower()
        cls = str(meta.get("cls") or "").strip().lower()
        label = f"{text} {aria}".strip().lower()
        if not re_primary.search(label):
            continue

        if any(tok in testid or tok in cls for tok in bad_tokens):
            continue

        score = 0
        for idx, token in enumerate(preferred):
            if token in label:
                score += (100 - idx * 10)
                break

        # Footer-ish buttons are typically lower and to the right.
        x = float(meta.get("x") or 0.0)
        y = float(meta.get("y") or 0.0)
        w = float(meta.get("w") or 0.0)
        h = float(meta.get("h") or 0.0)
        if y > 280:
            score += 20
        if x > 420:
            score += 10
        if w < 42 or h < 24:
            score -= 40
        if bool(meta.get("disabled")):
            score -= 10
        else:
            score += 3

        if score > best_score:
            best_score = score
            best_idx = i

    if best_idx >= 0:
        return buttons.nth(best_idx)

    # Fallback: pick the last visible role button by primary names within scope.
    for pat in [r"Submit application", r"Submit", r"Review", r"Next", r"Continue", r"Done"]:
        try:
            btns = scope.get_by_role("button", name=re.compile(pat, re.IGNORECASE))
            c = await btns.count()
            for idx in range(max(0, c - 1), -1, -1):
                b = btns.nth(idx)
                if await b.is_visible(timeout=100):
                    return b
        except Exception:
            continue
    return None


async def _extract_external_apply_url(page) -> str:
    """
    Best-effort: find an "Apply" link that goes to a non-LinkedIn site.
    This is useful when a job is not Easy Apply (so we can record the external path).
    """
    try:
        ext = page.locator("a[href^='http']").filter(has_text=re.compile(r"\bapply\b", re.IGNORECASE)).first
        if await ext.is_visible(timeout=1500):
            href = (await ext.get_attribute("href")) or ""
            if href and "linkedin.com" not in href:
                return href
    except Exception:
        return ""
    return ""


async def _scope_text(scope) -> str:
    """
    Best-effort: return text for both Locator and Page scopes.
    - Locator: inner_text()
    - Page: evaluate(document.body.innerText)
    """
    try:
        # Locator.inner_text(timeout=...) exists; Page.inner_text requires a selector.
        return await scope.inner_text(timeout=1500)
    except TypeError:
        # Likely a Page scope.
        try:
            return await scope.evaluate("() => (document.body?.innerText || '')")
        except Exception:
            return ""
    except Exception:
        try:
            return await scope.evaluate("() => (document.body?.innerText || '')")
        except Exception:
            return ""


async def _detect_submitted(scope) -> bool:
    # LinkedIn varies copy; keep it broad.
    low = (await _scope_text(scope) or "").lower()
    return (
        "application submitted" in low
        or "your application was sent" in low
        or "application sent" in low
        or ("submitted" in low and "application" in low)
    )


async def _page_detect_submitted(page) -> bool:
    """Fallback detection when the dialog disappears after submit."""
    try:
        text = await page.evaluate("() => (document.body?.innerText || '')")
    except Exception:
        return False
    low = (text or "").lower()

    # LinkedIn often shows "Applied 26m ago" on the job page instead of a modal confirmation.
    if re.search(
        r"\\bapplied\\s+\\d+\\s*(?:m|h|d|w|mo|minute|minutes|hour|hours|day|days|week|weeks|month|months)\\s+ago\\b",
        low,
        re.IGNORECASE,
    ):
        return True
    if "application submitted" in low:
        return True
    if "application status" in low and "submitted" in low:
        return True
    if "your application was sent" in low:
        return True
    return False


async def _wait_for_apply_ui(page, timeout_ms: int = 15_000) -> bool:
    """
    Easy Apply UI can be a modal without role=dialog (LinkedIn SDUI) or a full apply page.
    We treat the UI as "present" if any common form field or action button becomes visible.
    """
    deadline = asyncio.get_running_loop().time() + (timeout_ms / 1000.0)
    while asyncio.get_running_loop().time() < deadline:
        try:
            dlg = page.get_by_role("dialog").first
            if await dlg.is_visible(timeout=500):
                return True
        except Exception:
            pass
        for lab in ["First name", "Last name", "Mobile phone number", "Email address"]:
            try:
                loc = page.get_by_label(lab, exact=False).first
                if await loc.is_visible(timeout=500):
                    return True
            except Exception:
                pass
        try:
            btn = page.get_by_role("button", name=re.compile(r"(Next|Review|Submit application|Submit)", re.IGNORECASE)).first
            if await btn.is_visible(timeout=500):
                return True
        except Exception:
            pass
        await page.wait_for_timeout(400)
    return False


async def _get_apply_scope(page):
    # Prefer a dialog scope when present; otherwise operate on the whole page.
    try:
        dlg = page.get_by_role("dialog").first
        if await dlg.is_visible(timeout=800):
            return dlg
    except Exception:
        pass
    return page


async def _walk_easy_apply_steps(
    *,
    root: Path,
    page,
    candidate: Candidate,
    resume_path: Path,
    max_steps: int,
    submit: bool,
    db_conn=None,
    profile: Optional[Dict[str, str]] = None,
) -> Tuple[str, str]:
    """
    Walk the Easy Apply modal steps until submitted or we need manual help.
    """
    profile = profile or {}
    scope = await _get_apply_scope(page)
    resume_checked_once = False
    last_step_sig = ""
    same_step_sig_count = 0

    for step in range(max_steps):
        if is_checkpoint_url(page.url):
            await dump_debug(root, page, "apply_checkpoint")
            return ("failed", "checkpoint")

        # Page-level success detection (sometimes the modal closes instantly after submit).
        if await _page_detect_submitted(page):
            return ("submitted", "page_detected_submitted")

        # If the dialog closes (common after submit), check if we actually submitted.
        try:
            if scope is not page and await page.get_by_role("dialog").count() == 0:
                if await _page_detect_submitted(page):
                    return ("submitted", "dialog_closed_page_detected_submitted")
                # Give the UI a moment; it may be transitioning between steps.
                await page.wait_for_timeout(2500)
                if await page.get_by_role("dialog").count() == 0:
                    if await _page_detect_submitted(page):
                        return ("submitted", "dialog_closed_page_detected_submitted_2")
                    await dump_debug(root, page, f"apply_dialog_gone_s{step+1}")
                    return ("needs_manual", "dialog_gone_no_success_text")
                scope = page.get_by_role("dialog").first
        except Exception:
            pass

        # Fill common contact info fields if present.
        await _fill_if_present(scope, "First name", candidate.first_name)
        await _fill_if_present(scope, "Last name", candidate.last_name)
        await _select_phone_country_if_present(scope, candidate.phone_country)
        await _fill_if_present(scope, "Mobile phone number", candidate.phone_number)
        await _fill_if_present(scope, "Email address", candidate.email)

        # Policy: skip applications that require photo/image upload.
        try:
            if await _has_required_photo_upload(scope):
                await dump_debug(root, page, f"apply_photo_required_s{step+1}")
                return ("needs_manual", "photo_required_skip")
        except Exception:
            pass

        # Try resume handling only until we confirm this flow already has a resume option.
        if not resume_checked_once:
            try:
                attached = await _attach_resume_if_possible(scope, resume_path)
                if attached:
                    resume_checked_once = True
                else:
                    scope_text = (await _scope_text(scope)).lower()
                    stem = resume_path.stem.lower()
                    if "resume" in scope_text and (stem in scope_text or ".pdf" in scope_text or ".docx" in scope_text):
                        resume_checked_once = True
            except Exception:
                pass

        # Additional Questions: fill what we can; stop if unknown required questions exist.
        try:
            _ok, missing = await _fill_additional_questions(scope, db_conn=db_conn, profile=profile)
        except Exception:
            missing = []

        if missing:
            await dump_debug(root, page, "apply_questions_missing")
            return (
                "needs_manual",
                json.dumps(
                    {"reason": "missing_required_questions", "step": step + 1, "missing": missing},
                    ensure_ascii=False,
                ),
            )

        if await _detect_submitted(scope):
            return ("submitted", "detected_submitted_text")

        btn = await _find_primary_button(scope)
        if btn is None:
            # Sometimes the footer/button renders after a short delay (loading spinners).
            await page.wait_for_timeout(900)
            btn = await _find_primary_button(scope)
        if btn is None and scope is page:
            # Full-page apply flow fallback.
            btn = await _find_primary_button(page)
        if btn is None:
            await dump_debug(root, page, f"apply_no_primary_button_s{step+1}")
            return ("needs_manual", "no_next_or_submit_button")

        # If we're about to submit, stop unless explicitly allowed.
        try:
            name = await btn.inner_text(timeout=1000)
        except Exception:
            name = ""

        # If we keep seeing the same step signature, stop early and ask for manual help.
        try:
            st = await _scope_text(scope)
        except Exception:
            st = ""
        pm = re.search(r"(\\d{1,3})\\s*%", st or "")
        progress = pm.group(1) if pm else ""
        sig = f"{progress}|{(name or '').strip().lower()}"
        if sig and sig == last_step_sig:
            same_step_sig_count += 1
        else:
            last_step_sig = sig
            same_step_sig_count = 0
        if same_step_sig_count >= 2:
            await dump_debug(root, page, f"apply_stuck_repeating_s{step+1}")
            return ("needs_manual", "stuck_repeating_step")

        if re.search(r"submit", name or "", re.IGNORECASE) and not submit:
            await dump_debug(root, page, f"apply_reached_submit_s{step+1}")
            return ("needs_manual", "reached_submit_but_submit_disabled")

        try:
            if not await btn.is_enabled():
                # Try to detect any required fields we missed (for actionable debug).
                try:
                    req = await _extract_required_questions(scope)
                except Exception:
                    req = []
                await dump_debug(root, page, f"apply_button_disabled_s{step+1}")
                if req:
                    return (
                        "needs_manual",
                        json.dumps(
                            {"reason": "primary_button_disabled_required_fields", "step": step + 1, "missing": req},
                            ensure_ascii=False,
                        ),
                    )
                return ("needs_manual", "primary_button_disabled")
        except Exception:
            pass

        # Human-ish delay.
        await page.wait_for_timeout(random.randint(600, 1200))
        try:
            await btn.scroll_into_view_if_needed()
        except Exception:
            pass
        clicked = False
        click_err = ""
        for attempt in range(2):
            try:
                await btn.click(timeout=4000)
                clicked = True
                break
            except Exception as e:
                click_err = type(e).__name__
                # Modal may re-render between fill and click; re-find once.
                await page.wait_for_timeout(450)
                btn = await _find_primary_button(scope)
                if btn is None and scope is page:
                    btn = await _find_primary_button(page)
                if btn is None:
                    break

        if not clicked:
            try:
                # Final fallback: dispatch click on the selected button.
                if btn is not None:
                    await btn.evaluate("(el) => el.click()")
                    clicked = True
            except Exception:
                pass

        if not clicked:
            await dump_debug(root, page, f"apply_primary_click_failed_s{step+1}")
            return ("needs_manual", f"primary_click_failed:{click_err or 'unknown'}")
        await page.wait_for_timeout(random.randint(900, 1500))

        # If submit happened and the dialog closed, detect success.
        if scope is not page and await page.get_by_role("dialog").count() == 0 and await _page_detect_submitted(page):
            return ("submitted", "page_detected_submitted_after_click")

        # Refresh scope for the next step (some flows re-render the dialog).
        scope = await _get_apply_scope(page)

    await dump_debug(root, page, "apply_max_steps")
    return ("needs_manual", "max_steps_reached")


async def run_easy_apply_once(
    *,
    root: Path,
    page,
    job_url: str,
    candidate: Candidate,
    resume_path: Path,
    max_steps: int,
    submit: bool,
    db_conn=None,
    profile: Optional[Dict[str, str]] = None,
) -> Tuple[str, str]:
    """
    Returns (result, details):
      result: submitted | needs_manual | failed
    """
    ok = await goto_guarded(root=root, page=page, url=job_url, timeout_ms=30_000, tag_on_fail="apply_job_open_failed")
    if not ok:
        return ("failed", "job_open_failed_or_checkpoint")

    # If we've already applied, LinkedIn usually shows "Applied <time> ago" on the job page.
    if await _page_detect_submitted(page):
        return ("submitted", "already_applied_detected_on_job_page")

    # Click Easy Apply.
    await page.wait_for_timeout(1500)
    easy_a = page.locator("a[href*='openSDUIApplyFlow=true'], a[href*='/apply/?openSDUIApplyFlow=true']").first
    easy_btn = page.get_by_role("button", name=re.compile(r"easy apply", re.IGNORECASE)).first
    clicked = False
    apply_page = page

    async def _click_with_optional_popup(locator) -> None:
        nonlocal apply_page
        try:
            async with page.expect_popup(timeout=3000) as pop:
                await locator.click()
            apply_page = await pop.value
        except Exception:
            # No popup opened; keep using the same page.
            await locator.click()

    # Some job pages render an Easy Apply *link* that's hidden/un-clickable but still has
    # a valid apply URL. Prefer clicking when possible, otherwise navigate directly.
    easy_href = ""
    deadline = asyncio.get_running_loop().time() + 10.0
    while not clicked and asyncio.get_running_loop().time() < deadline:
        try:
            if await easy_a.is_visible(timeout=600):
                await _click_with_optional_popup(easy_a)
                clicked = True
                break
        except Exception:
            pass
        try:
            if await easy_btn.is_visible(timeout=600):
                await _click_with_optional_popup(easy_btn)
                clicked = True
                break
        except Exception:
            pass

        if not easy_href:
            try:
                href = (await easy_a.get_attribute("href")) or ""
                if href:
                    if href.startswith("/"):
                        href = "https://www.linkedin.com" + href
                    easy_href = href.replace("&amp;", "&")
            except Exception:
                pass

        if easy_href:
            try:
                await page.goto(easy_href, wait_until="domcontentloaded", timeout=30_000)
                apply_page = page
                clicked = True
                break
            except Exception:
                # If navigation fails, keep waiting for the clickable UI.
                pass

        await page.wait_for_timeout(450)
    if not clicked:
        ext_url = await _extract_external_apply_url(page)
        await dump_debug(root, page, "apply_no_easy_apply")
        if ext_url:
            return ("needs_manual", f"no_easy_apply_external:{ext_url}")
        return ("needs_manual", "no_easy_apply")

    # If Easy Apply opened a popup, apply_page already points to it.
    if apply_page is not page:
        try:
            apply_page.set_default_timeout(30_000)
            apply_page.set_default_navigation_timeout(30_000)
            await apply_page.wait_for_load_state("domcontentloaded", timeout=15_000)
        except Exception:
            pass

    # Wait for the Easy Apply UI to appear (modal OR full apply page).
    if not await _wait_for_apply_ui(apply_page, timeout_ms=18_000):
        await dump_debug(root, apply_page, "apply_ui_not_found")
        return ("needs_manual", "apply_ui_not_found")

    # From here on, operate on the apply page (same tab or popup).
    page = apply_page

    return await _walk_easy_apply_steps(
        root=root,
        page=page,
        candidate=candidate,
        resume_path=resume_path,
        max_steps=max_steps,
        submit=submit,
        db_conn=db_conn,
        profile=profile,
    )


async def continue_easy_apply_current(
    *,
    root: Path,
    page,
    candidate: Candidate,
    resume_path: Path,
    max_steps: int,
    submit: bool,
    db_conn=None,
    profile: Optional[Dict[str, str]] = None,
) -> Tuple[str, str]:
    """
    Continue an already-open Easy Apply flow (modal is expected to be visible).
    Useful after the user fills missing answers manually.
    """
    if not await _wait_for_apply_ui(page, timeout_ms=10_000):
        await dump_debug(root, page, "apply_ui_not_found_continue")
        return ("needs_manual", "apply_ui_not_found")

    return await _walk_easy_apply_steps(
        root=root,
        page=page,
        candidate=candidate,
        resume_path=resume_path,
        max_steps=max_steps,
        submit=submit,
        db_conn=db_conn,
        profile=profile,
    )
