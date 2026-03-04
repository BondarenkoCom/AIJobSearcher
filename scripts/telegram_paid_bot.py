from __future__ import annotations

import argparse
import hashlib
import io
import os
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Set

from pypdf import PdfReader

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.activity_db import connect as db_connect, init_db  # noqa: E402
from src.apply_assistant import ApplyAssistant, ApplyAssistantError  # noqa: E402
from src.config import cfg_get, load_config, resolve_path  # noqa: E402
from src.email_sender import load_env_file  # noqa: E402
from src.offer_feed import build_offer_rows, get_offer_row_by_lead_id  # noqa: E402
from src.offer_profiles import OfferProfile, load_offer_profiles  # noqa: E402
from src.profile_store import normalize_person_name  # noqa: E402
from src.telegram_bot_api import TelegramApiError, TelegramBotApi  # noqa: E402
from src.telegram_paid_store import (  # noqa: E402
    BotUser,
    add_payment_and_grant_access,
    get_bot_analytics_summary,
    get_active_subscription,
    get_user_selected_offer,
    get_user_selected_stack,
    get_user_summary,
    log_llm_usage,
    log_bot_event,
    log_delivery,
    set_user_selected_offer,
    set_user_selected_stack,
    upsert_bot_user,
)


@dataclass
class BotSettings:
    token: str
    default_offer_slug: str
    bot_name: str
    db_path: Path
    offers_path: Path
    offers: Dict[str, OfferProfile]
    support_text: str
    terms_text: str
    admin_chat_id: int
    poll_timeout: int
    sleep_sec: float
    photo_url: str
    commands: List[Dict[str, str]]
    free_user_ids: Set[int]
    free_usernames: Set[str]
    admin_user_ids: Set[int]
    admin_usernames: Set[str]


@dataclass
class ResumeSession:
    resume_text: str = ""
    awaiting_text: bool = False
    updated_at_ts: float = 0.0


_RESUME_TTL_SEC = 2 * 60 * 60
_RESUME_SESSIONS: Dict[int, ResumeSession] = {}
_APPLY_ASSISTANT: ApplyAssistant | None = None
_AI_SCORE_TTL_SEC = 2 * 60 * 60
_AI_SCORE_CACHE: Dict[str, Dict[str, Any]] = {}


def _int_env(name: str, default: int) -> int:
    raw = str(os.getenv(name) or "").strip()
    if not raw:
        return int(default)
    try:
        return int(raw)
    except Exception:
        return int(default)


def _safe(value: Any) -> str:
    return str(value or "").strip()


def _display_name(*, username: str = "", first_name: str = "") -> str:
    name = normalize_person_name(_safe(first_name))
    if name:
        return name
    user = _safe(username).lstrip("@")
    if user:
        return user
    return "there"


def _parse_command(text: str) -> str:
    parts = _safe(text).split()
    if not parts:
        return ""
    cmd = parts[0].strip()
    if "@" in cmd:
        cmd = cmd.split("@", 1)[0]
    return cmd.lower()


def _command_arg(text: str) -> str:
    raw = _safe(text)
    parts = raw.split(maxsplit=1)
    if len(parts) < 2:
        return ""
    return _safe(parts[1])


def _parse_payload(payload: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for part in _safe(payload).split("|"):
        if "=" not in part:
            continue
        key, value = part.split("=", 1)
        key = key.strip().lower()
        value = value.strip()
        if key:
            out[key] = value
    return out


def _split_csv(raw: str) -> List[str]:
    out: List[str] = []
    for part in str(raw or "").replace("\n", ",").split(","):
        token = part.strip()
        if token and token not in out:
            out.append(token)
    return out


def _split_ints(raw: str) -> Set[int]:
    out: Set[int] = set()
    for token in _split_csv(raw):
        try:
            out.add(int(token))
        except Exception:
            continue
    return out


def _split_names(raw: str) -> Set[str]:
    out: Set[str] = set()
    for token in _split_csv(raw):
        name = token.strip().lstrip("@").lower()
        if name:
            out.add(name)
    return out


def _offer_bot_cfg(offer: OfferProfile) -> Dict[str, Any]:
    return dict(offer.bot or {})


def _offer_title(offer: OfferProfile) -> str:
    return _safe(offer.title) or _safe(_offer_bot_cfg(offer).get("display_name")) or offer.slug


def _offer_summary(offer: OfferProfile) -> str:
    return _safe(offer.summary)


def _offer_preview_limit(offer: OfferProfile) -> int:
    return max(1, int(_offer_bot_cfg(offer).get("preview_limit") or 3))


def _offer_full_limit(offer: OfferProfile) -> int:
    return max(1, int(_offer_bot_cfg(offer).get("full_limit") or 10))


def _offer_plans(offer: OfferProfile) -> List[Dict[str, Any]]:
    return list(_offer_bot_cfg(offer).get("plans") or [])


def _offer_emoji(offer: OfferProfile) -> str:
    mapping = {
        "qa_gig_hunter": "🧪",
        "software_engineering_hunter": "💻",
        "data_ai_hunter": "🤖",
        "cybersecurity_hunter": "🛡️",
        "devops_cloud_hunter": "☁️",
        "remote_job_hunter": "📡",
    }
    return mapping.get(offer.slug, "🎯")


def _selectable_offers(settings: BotSettings) -> List[OfferProfile]:
    out: List[OfferProfile] = []
    for offer in settings.offers.values():
        if bool(_offer_bot_cfg(offer).get("selectable", True)):
            out.append(offer)
    return out


def _resolve_offer(settings: BotSettings, offer_slug: str) -> OfferProfile:
    slug = _safe(offer_slug)
    offer = settings.offers.get(slug)
    if offer is not None:
        return offer
    return settings.offers[settings.default_offer_slug]


def _current_offer(conn, settings: BotSettings, *, user_id: int) -> OfferProfile:
    slug = get_user_selected_offer(conn, user_id=user_id)
    return _resolve_offer(settings, slug or settings.default_offer_slug)


def _plan_map(offer: OfferProfile) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for item in _offer_plans(offer):
        code = _safe(item.get("code")).lower()
        if code:
            out[code] = dict(item)
    return out


def _offer_stack_options(offer: OfferProfile) -> List[Dict[str, Any]]:
    raw = list(_offer_bot_cfg(offer).get("stack_options") or [])
    out: List[Dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        code = _safe(item.get("code")).lower()
        label = _safe(item.get("label"))
        tokens = [str(x).strip().lower() for x in item.get("match_any") or [] if str(x).strip()]
        if code and label:
            out.append({"code": code, "label": label, "match_any": tokens})
    return out


def _stack_option_map(offer: OfferProfile) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for item in _offer_stack_options(offer):
        code = _safe(item.get("code")).lower()
        if code:
            out[code] = dict(item)
    return out


def _current_stack_code(conn, *, user_id: int, offer: OfferProfile) -> str:
    code = _safe(get_user_selected_stack(conn, user_id=user_id, offer_slug=offer.slug)).lower()
    if code and code in _stack_option_map(offer):
        return code
    return ""


def _current_stack_option(conn, *, user_id: int, offer: OfferProfile) -> Dict[str, Any]:
    code = _current_stack_code(conn, user_id=user_id, offer=offer)
    return dict(_stack_option_map(offer).get(code) or {})


def _selected_stack_label(conn, *, user_id: int, offer: OfferProfile) -> str:
    item = _current_stack_option(conn, user_id=user_id, offer=offer)
    return _safe(item.get("label")) or "Any stack"


def _build_offer_selector_keyboard(settings: BotSettings, *, current_offer_slug: str) -> Dict[str, Any]:
    rows: List[List[Dict[str, str]]] = []
    for offer in _selectable_offers(settings):
        slug = offer.slug
        title = _offer_title(offer)
        prefix = "✅ " if slug == current_offer_slug else ""
        rows.append([{"text": f"{prefix}{_offer_emoji(offer)} {title}", "callback_data": f"choose:{slug}"}])
    rows.append([{"text": "⚙️ Stack", "callback_data": "stack_menu"}, {"text": "📦 Today", "callback_data": "today"}])
    rows.append([{"text": "Sources", "callback_data": "sources"}, {"text": "💳 Plans", "callback_data": "plans"}])
    return {"inline_keyboard": rows}


def _build_stack_selector_keyboard(settings: BotSettings, *, offer: OfferProfile, current_stack_code: str) -> Dict[str, Any]:
    rows: List[List[Dict[str, str]]] = [[{"text": "✅ Any stack" if not current_stack_code else "Any stack", "callback_data": "stack:any"}]]
    current: List[Dict[str, str]] = []
    for item in _offer_stack_options(offer):
        code = _safe(item.get("code")).lower()
        label = _safe(item.get("label"))
        prefix = "✅ " if code == current_stack_code else ""
        current.append({"text": f"{prefix}{label}", "callback_data": f"stack:{code}"})
        if len(current) == 2:
            rows.append(current)
            current = []
    if current:
        rows.append(current)
    rows.append([{"text": "🎯 Profession", "callback_data": "choose_menu"}, {"text": "📦 Today", "callback_data": "today"}])
    rows.append([{"text": "Sources", "callback_data": "sources"}])
    return {"inline_keyboard": rows}


def _build_plans_keyboard(settings: BotSettings, *, offer: OfferProfile) -> Dict[str, Any]:
    rows: List[List[Dict[str, str]]] = []
    current: List[Dict[str, str]] = []
    for item in _offer_plans(offer):
        code = _safe(item.get("code")).lower()
        stars = int(item.get("stars") or 0)
        title = _safe(item.get("title")) or code
        current.append({"text": f"⭐ {title} - {stars} XTR", "callback_data": f"buy:{offer.slug}:{code}"})
        if len(current) == 2:
            rows.append(current)
            current = []
    if current:
        rows.append(current)
    rows.append([{"text": "🎯 Profession", "callback_data": "choose_menu"}, {"text": "⚙️ Stack", "callback_data": "stack_menu"}])
    rows.append([{"text": "👀 Preview", "callback_data": "preview"}, {"text": "Sources", "callback_data": "sources"}])
    rows.append([{"text": "📦 Today", "callback_data": "today"}])
    return {"inline_keyboard": rows}


def _main_menu_keyboard(settings: BotSettings, *, offer: OfferProfile, has_access: bool) -> Dict[str, Any]:
    rows: List[List[Dict[str, str]]] = [
        [{"text": "🎯 Profession", "callback_data": "choose_menu"}, {"text": "⚙️ Stack", "callback_data": "stack_menu"}],
        [{"text": "👀 Preview", "callback_data": "preview"}, {"text": "💳 Plans", "callback_data": "plans"}],
    ]
    if has_access:
        rows.append([{"text": "📦 Today's shortlist", "callback_data": "today"}, {"text": "Sources", "callback_data": "sources"}])
    else:
        rows.append([{"text": "🔓 Unlock full shortlist", "callback_data": "plans"}, {"text": "Sources", "callback_data": "sources"}])
    return {"inline_keyboard": rows}


def _build_offer_selector_keyboard(settings: BotSettings, *, current_offer_slug: str) -> Dict[str, Any]:
    rows: List[List[Dict[str, str]]] = []
    for offer in _selectable_offers(settings):
        slug = offer.slug
        title = _offer_title(offer)
        prefix = "[Current]" if slug == current_offer_slug else ""
        label = " ".join([part for part in [prefix, _offer_emoji(offer), title] if part]).strip()
        rows.append([{"text": label, "callback_data": f"choose:{slug}"}])
    rows.append([{"text": "Stack", "callback_data": "stack_menu"}, {"text": "CV", "callback_data": "cv_menu"}])
    rows.append([{"text": "Today", "callback_data": "today"}])
    rows.append([{"text": "Sources", "callback_data": "sources"}, {"text": "Plans", "callback_data": "plans"}])
    return {"inline_keyboard": rows}


def _build_stack_selector_keyboard(settings: BotSettings, *, offer: OfferProfile, current_stack_code: str) -> Dict[str, Any]:
    rows: List[List[Dict[str, str]]] = [
        [{"text": "[Current] Any stack" if not current_stack_code else "Any stack", "callback_data": "stack:any"}]
    ]
    current: List[Dict[str, str]] = []
    for item in _offer_stack_options(offer):
        code = _safe(item.get("code")).lower()
        label = _safe(item.get("label"))
        prefix = "[Current]" if code == current_stack_code else ""
        current.append({"text": " ".join([part for part in [prefix, label] if part]).strip(), "callback_data": f"stack:{code}"})
        if len(current) == 2:
            rows.append(current)
            current = []
    if current:
        rows.append(current)
    rows.append([{"text": "Profession", "callback_data": "choose_menu"}, {"text": "CV", "callback_data": "cv_menu"}])
    rows.append([{"text": "Today", "callback_data": "today"}, {"text": "Sources", "callback_data": "sources"}])
    return {"inline_keyboard": rows}


def _build_plans_keyboard(settings: BotSettings, *, offer: OfferProfile) -> Dict[str, Any]:
    rows: List[List[Dict[str, str]]] = []
    current: List[Dict[str, str]] = []
    for item in _offer_plans(offer):
        code = _safe(item.get("code")).lower()
        stars = int(item.get("stars") or 0)
        title = _safe(item.get("title")) or code
        current.append({"text": f"{title} - {stars} XTR", "callback_data": f"buy:{offer.slug}:{code}"})
        if len(current) == 2:
            rows.append(current)
            current = []
    if current:
        rows.append(current)
    rows.append([{"text": "Profession", "callback_data": "choose_menu"}, {"text": "Stack", "callback_data": "stack_menu"}])
    rows.append([{"text": "Preview", "callback_data": "preview"}, {"text": "CV", "callback_data": "cv_menu"}])
    rows.append([{"text": "Sources", "callback_data": "sources"}])
    rows.append([{"text": "Today", "callback_data": "today"}])
    return {"inline_keyboard": rows}


def _main_menu_keyboard(settings: BotSettings, *, offer: OfferProfile, has_access: bool) -> Dict[str, Any]:
    rows: List[List[Dict[str, str]]] = [
        [{"text": "Profession", "callback_data": "choose_menu"}, {"text": "Stack", "callback_data": "stack_menu"}],
        [{"text": "Preview", "callback_data": "preview"}, {"text": "CV", "callback_data": "cv_menu"}],
    ]
    if has_access:
        rows.append([{"text": "Today's shortlist", "callback_data": "today"}, {"text": "Sources", "callback_data": "sources"}])
        rows.append([{"text": "Plans", "callback_data": "plans"}])
    else:
        rows.append([{"text": "Unlock full shortlist", "callback_data": "plans"}, {"text": "Sources", "callback_data": "sources"}])
    return {"inline_keyboard": rows}


def _why_selected(row: Dict[str, Any]) -> str:
    text = f"{_safe(row.get('title'))} {_safe(row.get('snippet'))}".lower()
    why: List[str] = []
    if any(k in text for k in ("qa", "quality assurance", "sdet", "test automation")):
        why.append("QA/test signal")
    if any(k in text for k in ("software engineer", "software developer", "backend", "frontend", "full stack", "python", "c#", ".net", "react", "node")):
        why.append("software signal")
    if any(k in text for k in ("data engineer", "data scientist", "machine learning", "ml engineer", "ai engineer", "analytics", "sql")):
        why.append("data/AI signal")
    if any(k in text for k in ("security engineer", "application security", "appsec", "cybersecurity", "security analyst", "soc", "pentest")):
        why.append("security signal")
    if any(k in text for k in ("devops", "site reliability", "sre", "platform engineer", "cloud engineer", "kubernetes", "terraform", "aws", "gcp", "azure")):
        why.append("platform/cloud signal")
    if "remote" in text or "worldwide" in text or "anywhere" in text:
        why.append("remote")
    if _safe(row.get("contact_method")) == "email":
        why.append("direct email")
    if not why:
        why.append("filtered fit")
    return ", ".join(why[:4])


def _format_card(
    row: Dict[str, Any],
    *,
    index: int,
    has_access: bool,
    resume_loaded: bool,
) -> str:
    stack_hits = [str(x).strip() for x in row.get("stack_hits") or [] if str(x).strip()]
    lines = [
        f"{index}. {_safe(row.get('title'))}",
        f"Platform: {_safe(row.get('platform')) or '-'}",
        f"Type: {_safe(row.get('lead_type')) or '-'}",
        f"Location: {_safe(row.get('location')) or 'Remote/unspecified'}",
        f"Collected: {_collected_date_label(row.get('created_at'))}",
        f"Contact: {_safe(row.get('contact_method')) or '-'}",
    ]
    if has_access and resume_loaded:
        lines.append(f"Score: {_safe(row.get('display_score')) or '-'}")
    if stack_hits:
        lines.append(f"Stack: {', '.join(stack_hits[:6])}")
    lines.extend(
        [
        f"Why it fits: {_why_selected(row)}",
        f"Link: {_safe(row.get('url'))}",
        ]
    )
    if has_access:
        if resume_loaded:
            lines.append(f"Apply analysis: tap Analyze #{index} below")
        else:
            lines.append(f"Apply analysis: upload CV first, then tap Analyze #{index}")
    else:
        lines.append("Apply analysis: paid feature")
    return "\n".join(lines)


def _is_free_user(settings: BotSettings, *, user_id: int, username: str) -> bool:
    if int(user_id or 0) in settings.free_user_ids:
        return True
    uname = _safe(username).lstrip("@").lower()
    return bool(uname) and uname in settings.free_usernames


def _is_named_admin(settings: BotSettings, *, user_id: int, username: str) -> bool:
    if int(user_id or 0) in settings.admin_user_ids:
        return True
    uname = _safe(username).lstrip("@").lower()
    return bool(uname) and uname in settings.admin_usernames


def _has_privileged_access(settings: BotSettings, *, user_id: int, username: str) -> bool:
    return _is_free_user(settings, user_id=user_id, username=username) or _is_named_admin(
        settings,
        user_id=user_id,
        username=username,
    )


def _has_offer_access(settings: BotSettings, conn, *, user_id: int, username: str, offer_slug: str) -> bool:
    if _has_privileged_access(settings, user_id=user_id, username=username):
        return True
    return get_active_subscription(conn, user_id=user_id, offer_slug=offer_slug) is not None


def _is_admin(settings: BotSettings, *, user_id: int, chat_id: int, username: str) -> bool:
    admin_id = int(settings.admin_chat_id or 0)
    if admin_id and (int(user_id or 0) == admin_id or int(chat_id or 0) == admin_id):
        return True
    return _has_privileged_access(settings, user_id=user_id, username=username)


def _track_event(
    conn,
    *,
    user_id: int,
    chat_id: int,
    offer_slug: str,
    event_type: str,
    status: str = "ok",
    details: Dict[str, Any] | None = None,
) -> None:
    log_bot_event(
        conn,
        user_id=user_id,
        chat_id=chat_id,
        offer_slug=offer_slug,
        event_type=event_type,
        status=status,
        details=details,
    )


def _format_admin_stats(settings: BotSettings, summary: Dict[str, Any]) -> str:
    lines = [
        f"{settings.bot_name} admin stats",
        "",
        "Users",
        f"- Unique users total: {int(summary.get('users_total') or 0)}",
        f"- Active users 24h: {int(summary.get('active_users_24h') or 0)}",
        f"- Active users 7d: {int(summary.get('active_users_7d') or 0)}",
        f"- Returning users: {int(summary.get('returning_users') or 0)}",
        "",
        "Funnel",
        f"- Starts: {int((summary.get('starts') or {}).get('total') or 0)} total / {int((summary.get('starts') or {}).get('unique_users') or 0)} unique",
        f"- Plans opened: {int((summary.get('plans_opened') or {}).get('total') or 0)} / {int((summary.get('plans_opened') or {}).get('unique_users') or 0)} unique",
        f"- Buy clicks: {int((summary.get('buy_clicked') or {}).get('total') or 0)} / {int((summary.get('buy_clicked') or {}).get('unique_users') or 0)} unique",
        f"- Invoices sent: {int((summary.get('invoices_sent') or {}).get('total') or 0)} / {int((summary.get('invoices_sent') or {}).get('unique_users') or 0)} unique",
        f"- Pre-checkout ok: {int((summary.get('pre_checkout_ok') or {}).get('total') or 0)}",
        f"- Pre-checkout failed: {int((summary.get('pre_checkout_fail') or {}).get('total') or 0)}",
        f"- Successful payments: {int(summary.get('payments_total') or 0)} / {int(summary.get('unique_payers') or 0)} unique payers",
        f"- Stars revenue: {int(summary.get('stars_revenue') or 0)} XTR",
        f"- Active subscriptions: {int(summary.get('active_subscriptions') or 0)}",
        "",
        "Delivery",
        f"- Preview deliveries: {int(summary.get('preview_deliveries') or 0)}",
        f"- Full shortlist deliveries: {int(summary.get('full_deliveries') or 0)}",
    ]

    top_users = list(summary.get("top_packs_by_users") or [])
    if top_users:
        lines.extend(["", "Top packs by unique users"])
        for row in top_users[:5]:
            slug = _safe(row.get("offer_slug"))
            offer = settings.offers.get(slug)
            title = _offer_title(offer) if offer is not None else slug or "-"
            lines.append(f"- {title}: {int(row.get('unique_users') or 0)} unique / {int(row.get('visits') or 0)} starts")

    top_payments = list(summary.get("top_packs_by_payments") or [])
    if top_payments:
        lines.extend(["", "Top packs by payments"])
        for row in top_payments[:5]:
            slug = _safe(row.get("offer_slug"))
            offer = settings.offers.get(slug)
            title = _offer_title(offer) if offer is not None else slug or "-"
            lines.append(f"- {title}: {int(row.get('payments') or 0)} payments / {int(row.get('stars') or 0)} XTR")

    llm_usage = dict(summary.get("llm_usage") or {})
    if llm_usage:
        lines.extend(
            [
                "",
                "LLM spend",
                f"- Total calls: {int(llm_usage.get('calls') or 0)}",
                f"- Prompt tokens: {int(llm_usage.get('prompt_tokens') or 0)}",
                f"- Completion tokens: {int(llm_usage.get('completion_tokens') or 0)}",
                f"- Total tokens: {int(llm_usage.get('total_tokens') or 0)}",
                f"- Estimated total spend: ${float(llm_usage.get('spend_usd') or 0.0):.4f}",
            ]
        )

    llm_by_model = list(summary.get("llm_by_model") or [])
    if llm_by_model:
        lines.extend(["", "LLM by model"])
        for row in llm_by_model[:8]:
            provider = _safe(row.get("provider")) or "-"
            model = _safe(row.get("model")) or "-"
            task_type = _safe(row.get("task_type")) or "-"
            lines.append(
                f"- {provider}/{model} [{task_type}]: {int(row.get('calls') or 0)} calls / "
                f"{int(row.get('total_tokens') or 0)} tokens / ${float(row.get('spend_usd') or 0.0):.4f}"
            )

    return "\n".join(lines)


def _resume_session(user_id: int) -> ResumeSession:
    session = _RESUME_SESSIONS.get(int(user_id))
    now = time.time()
    if session is None or (session.updated_at_ts and (now - float(session.updated_at_ts)) > _RESUME_TTL_SEC):
        session = ResumeSession()
        _RESUME_SESSIONS[int(user_id)] = session
    return session


def _store_transient_resume(*, user_id: int, resume_text: str) -> None:
    session = _resume_session(int(user_id))
    session.resume_text = _safe(resume_text)
    session.awaiting_text = False
    session.updated_at_ts = time.time()


def _clear_transient_resume(*, user_id: int) -> None:
    session = _resume_session(int(user_id))
    session.resume_text = ""
    session.awaiting_text = False
    session.updated_at_ts = time.time()


def _current_resume_text(*, user_id: int) -> str:
    return _safe(_resume_session(int(user_id)).resume_text)


def _resume_status_line(*, user_id: int) -> str:
    if _current_resume_text(user_id=user_id):
        return "Resume mode: temporary text loaded in memory only"
    return "Resume mode: not loaded"


def _assistant_ready() -> bool:
    return bool(_safe(os.getenv("OPENAI_API_KEY")) and _safe(os.getenv("XAI_API_KEY")))


def _get_apply_assistant() -> ApplyAssistant:
    global _APPLY_ASSISTANT
    if _APPLY_ASSISTANT is None:
        _APPLY_ASSISTANT = ApplyAssistant()
    return _APPLY_ASSISTANT


def _extract_resume_text_from_pdf_bytes(data: bytes) -> str:
    reader = PdfReader(io.BytesIO(data))
    parts: List[str] = []
    for page in reader.pages:
        try:
            parts.append(page.extract_text() or "")
        except Exception:
            continue
    return "\n".join([_safe(p) for p in parts if _safe(p)]).strip()


def _extract_resume_text_from_document(api: TelegramBotApi, *, document: Dict[str, Any]) -> str:
    file_id = _safe(document.get("file_id"))
    file_name = _safe(document.get("file_name")).lower()
    mime_type = _safe(document.get("mime_type")).lower()
    if not file_id:
        raise RuntimeError("Missing Telegram file id.")
    file_meta = api.get_file(file_id=file_id)
    file_path = _safe(file_meta.get("file_path"))
    if not file_path:
        raise RuntimeError("Telegram did not return a file path.")
    raw = api.download_file_bytes(file_path=file_path)
    if not raw:
        raise RuntimeError("Downloaded file is empty.")
    if mime_type == "application/pdf" or file_name.endswith(".pdf"):
        text = _extract_resume_text_from_pdf_bytes(raw)
        if not text:
            raise RuntimeError("Could not extract text from PDF.")
        return text
    if mime_type.startswith("text/") or file_name.endswith(".txt") or file_name.endswith(".md"):
        try:
            return raw.decode("utf-8")
        except UnicodeDecodeError:
            return raw.decode("latin-1", errors="ignore")
    raise RuntimeError("Only PDF, TXT, and MD resume files are supported in this MVP.")


def _looks_like_resume_document(document: Dict[str, Any]) -> bool:
    file_name = _safe(document.get("file_name")).lower()
    mime_type = _safe(document.get("mime_type")).lower()
    if not file_name and not mime_type:
        return False
    if not (file_name.endswith((".pdf", ".txt", ".md")) or mime_type in ("application/pdf", "text/plain", "text/markdown")):
        return False
    markers = ("cv", "resume", "curriculum", "bondarenko")
    if any(marker in file_name for marker in markers):
        return True
    return not file_name and mime_type == "application/pdf"


def _format_cover_body(text: str) -> str:
    parts = [part.strip() for part in re.split(r"(?<=[.!?])\s+", _safe(text)) if part.strip()]
    if not parts:
        return ""
    chunks: List[str] = []
    current: List[str] = []
    for part in parts:
        current.append(part)
        if len(current) == 2:
            chunks.append(" ".join(current))
            current = []
    if current:
        chunks.append(" ".join(current))
    return "\n\n".join(chunks)


def _resume_fingerprint(resume_text: str) -> str:
    value = _safe(resume_text)
    if not value:
        return "no_cv"
    return hashlib.sha1(value.encode("utf-8", errors="ignore")).hexdigest()[:16]


def _analysis_cache_key(*, user_id: int, offer_slug: str, lead_id: str, stack_label: str, resume_text: str) -> str:
    return "|".join(
        [
            str(int(user_id or 0)),
            _safe(offer_slug),
            _safe(lead_id),
            _safe(stack_label),
            _resume_fingerprint(resume_text),
        ]
    )


def _analysis_cache_get(*, user_id: int, offer_slug: str, lead_id: str, stack_label: str, resume_text: str) -> Dict[str, Any] | None:
    key = _analysis_cache_key(
        user_id=user_id,
        offer_slug=offer_slug,
        lead_id=lead_id,
        stack_label=stack_label,
        resume_text=resume_text,
    )
    item = _AI_SCORE_CACHE.get(key) or {}
    ts = float(item.get("ts") or 0.0)
    if not ts or (time.time() - ts) > _AI_SCORE_TTL_SEC:
        _AI_SCORE_CACHE.pop(key, None)
        return None
    return dict(item)


def _analysis_cache_put(
    *,
    user_id: int,
    offer_slug: str,
    lead_id: str,
    stack_label: str,
    resume_text: str,
    analysis: Dict[str, Any],
) -> None:
    key = _analysis_cache_key(
        user_id=user_id,
        offer_slug=offer_slug,
        lead_id=lead_id,
        stack_label=stack_label,
        resume_text=resume_text,
    )
    _AI_SCORE_CACHE[key] = {
        "ts": time.time(),
        "analysis": dict(analysis),
    }


def _collected_date_label(value: Any) -> str:
    raw = _safe(value)
    if not raw:
        return "-"
    return raw.split("T", 1)[0]


def _extract_lead_index_from_card_text(text: str) -> int:
    match = re.match(r"^\s*(\d+)\.\s+", _safe(text))
    if not match:
        return 0
    try:
        return int(match.group(1))
    except Exception:
        return 0


def _row_matches_stack(row: Dict[str, Any], *, stack_option: Dict[str, Any]) -> bool:
    tokens = [str(x).strip().lower() for x in stack_option.get("match_any") or [] if str(x).strip()]
    if not tokens:
        return False
    stack_hits = [str(x).strip().lower() for x in row.get("stack_hits") or [] if str(x).strip()]
    text = f"{_safe(row.get('title'))} {_safe(row.get('snippet'))}".lower()
    for token in tokens:
        if token in stack_hits or token in text:
            return True
    return False


def _apply_stack_preference(
    rows: List[Dict[str, Any]],
    *,
    offer: OfferProfile,
    stack_code: str,
    limit: int,
) -> tuple[List[Dict[str, Any]], int, bool]:
    code = _safe(stack_code).lower()
    if not code:
        trimmed = list(rows[: max(1, int(limit))])
        return trimmed, len(trimmed), False

    stack_option = _stack_option_map(offer).get(code)
    if not stack_option:
        trimmed = list(rows[: max(1, int(limit))])
        return trimmed, len(trimmed), False

    matched: List[Dict[str, Any]] = []
    fallback: List[Dict[str, Any]] = []
    for row in rows:
        if _row_matches_stack(row, stack_option=stack_option):
            matched.append(row)
        else:
            fallback.append(row)

    chosen = list(matched[: max(1, int(limit))])
    if len(chosen) < int(limit):
        chosen.extend(fallback[: max(0, int(limit) - len(chosen))])
    broadened = len(chosen) > len(matched[: max(1, int(limit))])
    return chosen[: max(1, int(limit))], len(matched), broadened


def _build_chunk_keyboard(
    block: List[Dict[str, Any]],
    *,
    start_index: int,
    has_access: bool,
) -> Dict[str, Any] | None:
    if not has_access:
        return None
    rows: List[List[Dict[str, str]]] = []
    for idx, row in enumerate(block, start=start_index):
        buttons: List[Dict[str, str]] = [
            {"text": f"🎯 Analyze #{idx}", "callback_data": f"apply_idx:{idx}"}
        ]
        url = _safe(row.get("url"))
        if url:
            buttons.append({"text": f"🔗 Open #{idx}", "url": url})
        rows.append(buttons)
    return {"inline_keyboard": rows}


def _chunk_blocks(
    rows: List[Dict[str, Any]],
    *,
    chunk_size: int,
    has_access: bool,
    resume_loaded: bool,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    size = max(1, int(chunk_size))
    for start in range(0, len(rows), size):
        block = rows[start:start + size]
        parts = [
            _format_card(
                row,
                index=start + idx + 1,
                has_access=has_access,
                resume_loaded=resume_loaded,
            )
            for idx, row in enumerate(block)
        ]
        out.append(
            {
                "text": "\n\n".join(parts),
                "reply_markup": _build_chunk_keyboard(
                    block,
                    start_index=start + 1,
                    has_access=has_access,
                ),
            }
        )
    return out


def _delivery_label(delivery_kind: str) -> str:
    mapping = {
        "preview": "preview",
        "member_full": "full access",
        "paid_full": "full access",
    }
    return mapping.get(_safe(delivery_kind), _safe(delivery_kind))


def _personalized_rows(
    conn,
    settings: BotSettings,
    *,
    offer: OfferProfile,
    user_id: int,
    limit: int,
    delivery_kind: str,
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]], str, int, bool]:
    stack_code = _current_stack_code(conn, user_id=user_id, offer=offer)
    stack_limit = 5 if stack_code else limit
    rows_all = _get_rows(settings, offer=offer, limit=max(limit, stack_limit))
    rows, matched_count, broadened = _apply_stack_preference(
        rows_all,
        offer=offer,
        stack_code=stack_code,
        limit=stack_limit if delivery_kind != "preview" else min(stack_limit, limit),
    )
    return rows, rows_all, stack_code, matched_count, broadened


def _resolve_shortlist_row(
    conn,
    settings: BotSettings,
    *,
    offer: OfferProfile,
    user_id: int,
    lead_index: int,
) -> tuple[Dict[str, Any], int]:
    rows, _rows_all, _stack_code, _matched_count, _broadened = _personalized_rows(
        conn,
        settings,
        offer=offer,
        user_id=user_id,
        limit=_offer_full_limit(offer),
        delivery_kind="member_full",
    )
    if lead_index < 1 or lead_index > len(rows):
        return {}, len(rows)
    return dict(rows[lead_index - 1]), len(rows)


def _build_terms_text(settings: BotSettings) -> str:
    if settings.terms_text:
        return settings.terms_text
    return (
        "Terms\n"
        "- This bot sells access to filtered job/gig leads, not guaranteed employment.\n"
        "- Refund handling is manual via support.\n"
        "- Access duration starts after successful Telegram Stars payment.\n"
        "- Resume text sent via /cv is processed transiently in memory and is not stored in our DB or long-term files.\n"
        "- Links and availability may change on source platforms."
    )


def _build_support_text(settings: BotSettings) -> str:
    if settings.support_text:
        return settings.support_text
    return "Support is not configured yet. Set TELEGRAM_SUPPORT_TEXT or TELEGRAM_SUPPORT_HANDLE."


def _humanize_scanner_name(name: str) -> str:
    mapping = {
        "job_board_public": "Public remote job boards",
        "hn_remote": "Hacker News Who Is Hiring",
        "workana": "Workana",
        "freelancermap": "Freelancermap",
        "reddit": "Reddit gigs",
        "upwork": "Upwork",
        "telegram": "Telegram gigs",
        "linkedin_jobs": "LinkedIn jobs",
        "linkedin_posts": "LinkedIn posts",
    }
    key = _safe(name)
    if key in mapping:
        return mapping[key]
    return key.replace("_", " ").strip().title()


def _humanize_source_name(source: str) -> str:
    value = _safe(source)
    mapping = {
        "remotive_api": "Remotive API",
        "arbeitnow_api": "Arbeitnow API",
        "remoteok_api": "RemoteOK API",
        "jobicy_api": "Jobicy API",
        "reddit": "Reddit",
        "telegram": "Telegram",
        "linkedin": "LinkedIn",
        "hn": "Hacker News",
        "job_board": "Public job boards",
        "workana.com": "Workana",
        "freelancermap.com": "Freelancermap",
    }
    if value in mapping:
        return mapping[value]
    if value.startswith("hn_whoishiring"):
        return "Hacker News Who Is Hiring"
    if value.startswith("public_scan:workana"):
        return "Workana public scan"
    if value.startswith("public_scan:"):
        return f"Public scan: {value.split(':', 1)[1]}"
    return value


def _count_values(rows: List[Dict[str, Any]], key: str) -> List[tuple[str, int]]:
    counts: Dict[str, int] = {}
    for row in rows:
        value = _safe(row.get(key))
        if not value:
            continue
        counts[value] = int(counts.get(value) or 0) + 1
    ordered = sorted(counts.items(), key=lambda item: (-int(item[1]), item[0]))
    return ordered


def _get_rows(settings: BotSettings, *, offer: OfferProfile, limit: int) -> List[Dict[str, Any]]:
    conn = db_connect(settings.db_path)
    try:
        init_db(conn)
        fetch_limit = max(int(limit) * 8, 24)
        return build_offer_rows(conn, offer=offer, scan_limit=max(500, fetch_limit * 4), limit=fetch_limit)
    finally:
        conn.close()


def _send_offer_picker(api: TelegramBotApi, settings: BotSettings, *, chat_id: int, current_offer: OfferProfile) -> None:
    lines = [
        f"{settings.bot_name}",
        f"Current pack: {_offer_title(current_offer)}",
        "",
        "🎯 Choose the profession pack you want to hunt.",
        "The scanner and shortlist change with the selected pack.",
    ]
    api.send_message(
        chat_id=chat_id,
        text="\n".join(lines),
        reply_markup=_build_offer_selector_keyboard(settings, current_offer_slug=current_offer.slug),
    )


def _send_stack_picker(api: TelegramBotApi, conn, settings: BotSettings, *, chat_id: int, user_id: int, current_offer: OfferProfile, username: str) -> None:
    if not _offer_stack_options(current_offer):
        api.send_message(
            chat_id=chat_id,
            text=(
                f"{_offer_title(current_offer)}\n"
                "Stack filter is not configured for this pack yet."
            ),
            reply_markup=_main_menu_keyboard(
                settings,
                offer=current_offer,
                has_access=_has_offer_access(settings, conn, user_id=user_id, username=username, offer_slug=current_offer.slug),
            ),
        )
        return
    lines = [
        f"{settings.bot_name}",
        f"Current pack: {_offer_title(current_offer)}",
        f"Current stack: {_selected_stack_label(conn, user_id=user_id, offer=current_offer)}",
        "",
        "Choose a stack focus.",
        "Exact stack hits are prioritized first. If they are too few, the shortlist is filled with broader fit.",
    ]
    api.send_message(
        chat_id=chat_id,
        text="\n".join(lines),
        reply_markup=_build_stack_selector_keyboard(
            settings,
            offer=current_offer,
            current_stack_code=_current_stack_code(conn, user_id=user_id, offer=current_offer),
        ),
    )


def _send_cv_prompt(
    api: TelegramBotApi,
    conn,
    settings: BotSettings,
    *,
    chat_id: int,
    user_id: int,
    username: str,
    first_name: str,
    current_offer: OfferProfile,
) -> None:
    session = _resume_session(user_id)
    session.awaiting_text = True
    session.updated_at_ts = time.time()
    has_resume = bool(_current_resume_text(user_id=user_id))
    lines = [
        f"{_display_name(username=username, first_name=first_name)}, send your CV now.",
        "You can send plain text, PDF, TXT, or MD.",
        "I will parse it and keep it only in temporary bot memory, not in our DB or long-term files.",
    ]
    if has_resume:
        lines.append("Current status: a temporary CV is already loaded and will be replaced by the next one you send.")
    api.send_message(
        chat_id=chat_id,
        text="\n".join(lines),
        reply_markup=_main_menu_keyboard(
            settings,
            offer=current_offer,
            has_access=_has_offer_access(settings, conn, user_id=user_id, username=username, offer_slug=current_offer.slug),
        ),
    )


def _send_sources(
    api: TelegramBotApi,
    conn,
    settings: BotSettings,
    *,
    offer: OfferProfile,
    user_id: int,
    username: str,
    chat_id: int,
) -> None:
    rows, rows_all, stack_code, matched_count, broadened = _personalized_rows(
        conn,
        settings,
        offer=offer,
        user_id=user_id,
        limit=_offer_full_limit(offer),
        delivery_kind="member_full",
    )
    stack_label = _selected_stack_label(conn, user_id=user_id, offer=offer)
    configured_required = [_humanize_scanner_name(_safe(item.get("name"))) for item in offer.scanners if not bool(item.get("optional", False))]
    configured_optional = [_humanize_scanner_name(_safe(item.get("name"))) for item in offer.scanners if bool(item.get("optional", False))]
    source_counts = _count_values(rows, "source")
    platform_counts = _count_values(rows, "platform")
    newest_ts = max((_safe(row.get("created_at")) for row in rows_all), default="")
    oldest_ts = min((_safe(row.get("created_at")) for row in rows), default="")

    lines: List[str] = [
        f"{_offer_title(offer)} sources",
        f"Current stack: {stack_label}",
        "",
        f"Latest shortlist snapshot: {newest_ts or '-'}",
        f"Rows in current shortlist: {len(rows)}",
    ]
    if stack_code:
        if matched_count > 0 and not broadened:
            lines.append(f"Stack matching: {matched_count} exact hits")
        elif matched_count > 0 and broadened:
            lines.append(f"Stack matching: {matched_count} exact hits, shortlist broadened")
        else:
            lines.append("Stack matching: no exact hits, shortlist broadened")
    if oldest_ts:
        lines.append(f"Oldest row in this shortlist: {oldest_ts}")

    if configured_required:
        lines.extend(["", "Configured scanners (required)"])
        for label in configured_required[:8]:
            lines.append(f"- {label}")
    if configured_optional:
        lines.extend(["", "Configured scanners (optional when enabled)"])
        for label in configured_optional[:8]:
            lines.append(f"- {label}")

    if source_counts:
        lines.extend(["", "Sources in this shortlist"])
        for source, count in source_counts[:8]:
            lines.append(f"- {_humanize_source_name(source)}: {count}")

    if platform_counts:
        lines.extend(["", "Platforms in this shortlist"])
        for platform, count in platform_counts[:8]:
            lines.append(f"- {_humanize_source_name(platform)}: {count}")

    api.send_message(
        chat_id=chat_id,
        text="\n".join(lines),
        reply_markup=_main_menu_keyboard(
            settings,
            offer=offer,
            has_access=_has_offer_access(settings, conn, user_id=user_id, username=username, offer_slug=offer.slug),
        ),
    )


def _send_feed(
    *,
    api: TelegramBotApi,
    conn,
    settings: BotSettings,
    offer: OfferProfile,
    user_id: int,
    username: str,
    chat_id: int,
    limit: int,
    delivery_kind: str,
) -> None:
    rows, rows_all, stack_code, matched_count, broadened = _personalized_rows(
        conn=conn,
        settings=settings,
        offer=offer,
        user_id=user_id,
        limit=limit,
        delivery_kind=delivery_kind,
    )
    has_access = _has_offer_access(settings, conn, user_id=user_id, username=username, offer_slug=offer.slug)
    resume_text = _current_resume_text(user_id=user_id)
    resume_loaded = bool(resume_text)
    if not rows:
        msg = api.send_message(
            chat_id=chat_id,
            text=f"{_offer_title(offer)} feed is empty right now. Run the offer pipeline first, then try again.",
            reply_markup=_build_plans_keyboard(settings, offer=offer),
        )
        log_delivery(
            conn,
            user_id=user_id,
            offer_slug=offer.slug,
            delivery_kind=delivery_kind,
            item_count=0,
            message_id=msg.get("message_id"),
            details={"reason": "empty_feed"},
        )
        return

    if has_access and resume_loaded and _assistant_ready():
        stack_label = _selected_stack_label(conn, user_id=user_id, offer=offer)
        api.send_chat_action(chat_id=chat_id, action="typing")
        api.send_message(
            chat_id=chat_id,
            text=(
                "⏳ Building your scored shortlist.\n"
                "I am matching these leads against your current stack and temporary CV."
            ),
        )
        scored_rows: List[Dict[str, Any]] = []
        for row in rows:
            row_copy = dict(row)
            try:
                analysis, _ = _get_cached_or_fresh_analysis(
                    conn,
                    offer=offer,
                    user_id=user_id,
                    lead=row_copy,
                    stack_label=stack_label,
                    resume_text=resume_text,
                    task_type="shortlist_score",
                )
                row_copy["display_score"] = int(analysis.get("match_score") or 0)
            except ApplyAssistantError:
                row_copy["display_score"] = ""
            scored_rows.append(row_copy)
        rows = scored_rows

    header = (
        f"{_offer_title(offer)}\n"
        f"Leads in this drop: {len(rows)}\n"
        f"Mode: {_delivery_label(delivery_kind)}"
    )
    stack_label = _selected_stack_label(conn, user_id=user_id, offer=offer)
    if stack_code:
        if matched_count <= 0:
            header += f"\nStack focus: {stack_label} (filled with broader fit)"
        elif broadened:
            header += f"\nStack focus: {stack_label} ({matched_count} exact hits, rest broadened)"
        else:
            header += f"\nStack focus: {stack_label}"
    first = api.send_message(
        chat_id=chat_id,
        text=header,
        reply_markup=_main_menu_keyboard(settings, offer=offer, has_access=has_access),
    )
    sent = 1
    chunk_size = 3 if delivery_kind == "preview" else 4
    for block in _chunk_blocks(rows, chunk_size=chunk_size, has_access=has_access, resume_loaded=resume_loaded):
        api.send_message(
            chat_id=chat_id,
            text=_safe(block.get("text")),
            reply_markup=block.get("reply_markup"),
        )
        sent += 1
    if has_access and resume_loaded:
        tip_lines = [
            "🎯 Apply Assistant",
            f"• Use /apply 1..{len(rows)} to analyze a lead from this shortlist",
            "• Use /cv to load a temporary CV for tailored cover notes",
            "• Use /forgetcv to wipe that temporary CV from bot memory",
        ]
    elif has_access:
        tip_lines = [
            "🎯 Apply Assistant",
            "• Upload your CV first with /cv to unlock real AI match scores",
            "• After that, each lead will show a scored fit and /apply command",
        ]
    else:
        tip_lines = [
            "🔒 AI Apply Assistant",
            "• AI match analysis and tailored cover notes are paid features",
            "• Open Plans to unlock full shortlist + AI access with Stars",
        ]
    api.send_message(chat_id=chat_id, text="\n".join(tip_lines))
    sent += 1
    log_delivery(
        conn,
        user_id=user_id,
        offer_slug=offer.slug,
        delivery_kind=delivery_kind,
        item_count=len(rows),
        message_id=first.get("message_id"),
        details={"messages_sent": sent, "stack_code": stack_code, "matched_count": matched_count, "broadened": broadened},
    )


def _send_preview_with_pitch(
    api: TelegramBotApi,
    conn,
    settings: BotSettings,
    *,
    offer: OfferProfile,
    user_id: int,
    username: str,
    chat_id: int,
) -> None:
    _send_feed(
        api=api,
        conn=conn,
        settings=settings,
        offer=offer,
        user_id=user_id,
        username=username,
        chat_id=chat_id,
        limit=_offer_preview_limit(offer),
        delivery_kind="preview",
    )
    if not _has_offer_access(settings, conn, user_id=user_id, username=username, offer_slug=offer.slug):
        _send_ai_paywall(
            api,
            settings,
            offer=offer,
            chat_id=chat_id,
            reason="Unlock full shortlist and AI Apply Assistant with Telegram Stars.",
        )
    return
    pitch_lines = [
        f"{_offer_title(offer)}",
        "🔓 Unlock full access with Telegram Stars:",
    ]
    for item in _offer_plans(offer):
        pitch_lines.append(f"- {_safe(item.get('title'))}: {int(item.get('stars') or 0)} XTR")
    api.send_message(
        chat_id=chat_id,
        text="\n".join(pitch_lines),
        reply_markup=_build_plans_keyboard(settings, offer=offer),
    )


def _send_ai_paywall(
    api: TelegramBotApi,
    settings: BotSettings,
    *,
    offer: OfferProfile,
    chat_id: int,
    reason: str = "",
) -> None:
    lines = [
        f"🔒 {_offer_title(offer)}",
        reason or "Unlock full access with Telegram Stars.",
    ]
    for item in _offer_plans(offer):
        lines.append(f"• {_safe(item.get('title'))}: {int(item.get('stars') or 0)} XTR")
    api.send_message(
        chat_id=chat_id,
        text="\n".join(lines),
        reply_markup=_build_plans_keyboard(settings, offer=offer),
    )


def _send_status(
    api: TelegramBotApi,
    conn,
    settings: BotSettings,
    *,
    offer: OfferProfile,
    user_id: int,
    username: str,
    chat_id: int,
) -> None:
    summary = get_user_summary(conn, user_id=user_id, offer_slug=offer.slug)
    active = summary.get("active_subscription")
    stack_label = _selected_stack_label(conn, user_id=user_id, offer=offer)
    if _is_free_user(settings, user_id=user_id, username=username):
        text = (
            f"Current pack: {_offer_title(offer)}\n"
            f"Current stack: {stack_label}\n"
            "Status: free test access\n"
            f"{_resume_status_line(user_id=user_id)}\n"
            f"Payments: {summary.get('payments_count')}\n"
            f"Deliveries: {summary.get('deliveries_count')}"
        )
        has_access = True
    elif active:
        text = (
            f"Current pack: {_offer_title(offer)}\n"
            f"Current stack: {stack_label}\n"
            "Status: active\n"
            f"Plan: {_safe(active.get('plan_code'))}\n"
            f"Access until: {_safe(active.get('ends_at'))}\n"
            f"{_resume_status_line(user_id=user_id)}\n"
            f"Payments: {summary.get('payments_count')}\n"
            f"Deliveries: {summary.get('deliveries_count')}"
        )
        has_access = True
    else:
        text = (
            f"Current pack: {_offer_title(offer)}\n"
            f"Current stack: {stack_label}\n"
            "Status: no active access\n"
            f"{_resume_status_line(user_id=user_id)}\n"
            f"Payments: {summary.get('payments_count')}\n"
            f"Deliveries: {summary.get('deliveries_count')}"
        )
        has_access = False
    api.send_message(chat_id=chat_id, text=text, reply_markup=_main_menu_keyboard(settings, offer=offer, has_access=has_access))


def _apply_keyboard(*, lead: Dict[str, Any]) -> Dict[str, Any]:
    rows: List[List[Dict[str, str]]] = []
    if _safe(lead.get("url")):
        rows.append([{"text": "Open job", "url": _safe(lead.get("url"))}])
    rows.append([{"text": "Generate cover", "callback_data": f"cover:{_safe(lead.get('lead_id'))}"}])
    rows.append([{"text": "Today", "callback_data": "today"}, {"text": "Sources", "callback_data": "sources"}])
    return {"inline_keyboard": rows}


def _send_cv_required(
    api: TelegramBotApi,
    conn,
    settings: BotSettings,
    *,
    offer: OfferProfile,
    user_id: int,
    username: str,
    first_name: str,
    chat_id: int,
    reason: str,
) -> None:
    session = _resume_session(user_id)
    session.awaiting_text = True
    session.updated_at_ts = time.time()
    api.send_message(
        chat_id=chat_id,
        text=(
            f"{reason}\n"
            "Send your CV now as text, PDF, TXT, or MD.\n"
            "I will keep it only in temporary bot memory, not in our DB or long-term files."
        ),
        reply_markup=_main_menu_keyboard(
            settings,
            offer=offer,
            has_access=_has_offer_access(settings, conn, user_id=user_id, username=username, offer_slug=offer.slug),
        ),
    )


def _get_cached_or_fresh_analysis(
    conn,
    *,
    offer: OfferProfile,
    user_id: int,
    lead: Dict[str, Any],
    stack_label: str,
    resume_text: str,
    task_type: str,
) -> tuple[Dict[str, Any], bool]:
    lead_id = _safe(lead.get("lead_id"))
    cached = _analysis_cache_get(
        user_id=user_id,
        offer_slug=offer.slug,
        lead_id=lead_id,
        stack_label=stack_label,
        resume_text=resume_text,
    )
    if cached:
        return dict(cached.get("analysis") or {}), True
    assistant = _get_apply_assistant()
    result = assistant.analyze_job(
        offer_title=_offer_title(offer),
        stack_label=stack_label,
        lead=lead,
        resume_text=resume_text,
    )
    analysis = {
        "match_score": int(result.match_score or 0),
        "strengths": list(result.strengths or []),
        "weaknesses": list(result.weaknesses or []),
        "pitch": _safe(result.pitch),
        "salary_hint": _safe(result.salary_hint),
    }
    _analysis_cache_put(
        user_id=user_id,
        offer_slug=offer.slug,
        lead_id=lead_id,
        stack_label=stack_label,
        resume_text=resume_text,
        analysis=analysis,
    )
    log_llm_usage(
        conn,
        user_id=user_id,
        offer_slug=offer.slug,
        lead_id=lead_id,
        provider=result.usage.provider,
        model=result.usage.model,
        task_type=task_type,
        prompt_tokens=result.usage.prompt_tokens,
        completion_tokens=result.usage.completion_tokens,
        total_tokens=result.usage.total_tokens,
        estimated_cost_usd=result.usage.estimated_cost_usd,
        details={"resume_loaded": bool(resume_text)},
    )
    return analysis, False


def _format_apply_analysis(
    *,
    offer: OfferProfile,
    stack_label: str,
    lead_index: int,
    lead: Dict[str, Any],
    analysis: Dict[str, Any],
    resume_loaded: bool,
) -> str:
    strengths = [str(x).strip() for x in analysis.get("strengths") or [] if str(x).strip()]
    weaknesses = [str(x).strip() for x in analysis.get("weaknesses") or [] if str(x).strip()]
    lines = [
        "🎯 Apply Assistant",
        f"{_offer_title(offer)}",
        "",
        f"Lead #{lead_index}",
        f"{_safe(lead.get('title'))}",
        f"Company: {_safe(lead.get('company')) or '-'}",
        f"Stack: {stack_label}",
        f"Match: {int(analysis.get('match_score') or 0)}%",
        f"Resume: {'CV loaded' if resume_loaded else 'No CV, using pack and stack only'}",
    ]
    if strengths:
        lines.append("")
        lines.append("Strong fit")
        for item in strengths[:5]:
            lines.append(f"• {item}")
    if weaknesses:
        lines.append("")
        lines.append("Gaps to watch")
        for item in weaknesses[:3]:
            lines.append(f"• {item}")
    pitch = _safe(analysis.get("pitch"))
    if pitch:
        lines.append("")
        lines.append("Suggested angle")
        lines.append(pitch)
    salary_hint = _safe(analysis.get("salary_hint"))
    if salary_hint:
        lines.append("")
        lines.append(f"Salary hint: {salary_hint}")
    return "\n".join(lines)


def _send_apply_package(
    api: TelegramBotApi,
    conn,
    settings: BotSettings,
    *,
    offer: OfferProfile,
    user_id: int,
    username: str,
    chat_id: int,
    lead_index: int,
) -> None:
    if not _has_offer_access(settings, conn, user_id=user_id, username=username, offer_slug=offer.slug):
        _track_event(
            conn,
            user_id=user_id,
            chat_id=chat_id,
            offer_slug=offer.slug,
            event_type="ai_paywall_shown",
            details={"origin": "apply", "lead_index": int(lead_index)},
        )
        _send_ai_paywall(
            api,
            settings,
            offer=offer,
            chat_id=chat_id,
            reason="Unlock full access to use AI match analysis and tailored cover notes.",
        )
        return
    lead, total = _resolve_shortlist_row(conn, settings, offer=offer, user_id=user_id, lead_index=lead_index)
    if not lead:
        api.send_message(
            chat_id=chat_id,
            text=f"Lead number is out of range. Use /today first, then /apply 1..{max(1, total)}.",
            reply_markup=_main_menu_keyboard(
                settings,
                offer=offer,
                has_access=_has_offer_access(settings, conn, user_id=user_id, username=username, offer_slug=offer.slug),
            ),
        )
        return
    if not _assistant_ready():
        api.send_message(chat_id=chat_id, text="Apply Assistant is not configured yet.")
        return
    resume_text = _current_resume_text(user_id=user_id)
    if not resume_text:
        _track_event(
            conn,
            user_id=user_id,
            chat_id=chat_id,
            offer_slug=offer.slug,
            event_type="cv_required_for_ai",
            details={"origin": "apply", "lead_index": int(lead_index)},
        )
        _send_cv_required(
            api,
            conn,
            settings,
            offer=offer,
            user_id=user_id,
            username=username,
            first_name="",
            chat_id=chat_id,
            reason="Upload your CV first so I can give you a real AI match score for this lead.",
        )
        return
    stack_label = _selected_stack_label(conn, user_id=user_id, offer=offer)
    api.send_chat_action(chat_id=chat_id, action="typing")
    api.send_message(
        chat_id=chat_id,
        text=(
            "⏳ Working on it.\n"
            "I am analyzing this lead against your pack, stack, and temporary CV."
        ),
    )
    try:
        analysis, _ = _get_cached_or_fresh_analysis(
            conn,
            offer=offer,
            user_id=user_id,
            lead=lead,
            resume_text=resume_text,
            stack_label=stack_label,
            task_type="analyze",
        )
    except ApplyAssistantError as exc:
        api.send_message(chat_id=chat_id, text=f"Apply Assistant error: {_safe(exc)}")
        return
    _track_event(
        conn,
        user_id=user_id,
        chat_id=chat_id,
        offer_slug=offer.slug,
        event_type="apply_analyzed",
        details={"lead_id": _safe(lead.get("lead_id")), "lead_index": int(lead_index)},
    )
    api.send_message(
        chat_id=chat_id,
        text=_format_apply_analysis(
            offer=offer,
            stack_label=stack_label,
            lead_index=lead_index,
            lead=lead,
            analysis=analysis,
            resume_loaded=bool(resume_text),
        ),
        reply_markup=_apply_keyboard(lead=lead),
    )


def _send_cover_for_lead(
    api: TelegramBotApi,
    conn,
    settings: BotSettings,
    *,
    offer: OfferProfile,
    user_id: int,
    username: str,
    chat_id: int,
    lead_id: str,
) -> None:
    if not _has_offer_access(settings, conn, user_id=user_id, username=username, offer_slug=offer.slug):
        _track_event(
            conn,
            user_id=user_id,
            chat_id=chat_id,
            offer_slug=offer.slug,
            event_type="ai_paywall_shown",
            details={"origin": "cover", "lead_id": lead_id},
        )
        _send_ai_paywall(
            api,
            settings,
            offer=offer,
            chat_id=chat_id,
            reason="Unlock full access to generate tailored cover notes with AI.",
        )
        return
    lead = get_offer_row_by_lead_id(conn, offer=offer, lead_id=lead_id)
    if not lead:
        api.send_message(chat_id=chat_id, text="That lead is no longer available in the current shortlist snapshot.")
        return
    if not _assistant_ready():
        api.send_message(chat_id=chat_id, text="Apply Assistant is not configured yet.")
        return
    assistant = _get_apply_assistant()
    resume_text = _current_resume_text(user_id=user_id)
    if not resume_text:
        _track_event(
            conn,
            user_id=user_id,
            chat_id=chat_id,
            offer_slug=offer.slug,
            event_type="cv_required_for_ai",
            details={"origin": "cover", "lead_id": lead_id},
        )
        _send_cv_required(
            api,
            conn,
            settings,
            offer=offer,
            user_id=user_id,
            username=username,
            first_name="",
            chat_id=chat_id,
            reason="Upload your CV first so I can generate a tailored cover note for this lead.",
        )
        return
    api.send_chat_action(chat_id=chat_id, action="typing")
    api.send_message(
        chat_id=chat_id,
        text=(
            "⏳ Working on it.\n"
            "I am generating a tailored cover note for this lead and your temporary CV."
        ),
    )
    try:
        cover = assistant.generate_cover(
            offer_title=_offer_title(offer),
            stack_label=_selected_stack_label(conn, user_id=user_id, offer=offer),
            lead=lead,
            resume_text=resume_text,
        )
    except ApplyAssistantError as exc:
        api.send_message(chat_id=chat_id, text=f"Cover generation error: {_safe(exc)}")
        return
    log_llm_usage(
        conn,
        user_id=user_id,
        offer_slug=offer.slug,
        lead_id=_safe(lead.get("lead_id")),
        provider=cover.usage.provider,
        model=cover.usage.model,
        task_type="cover",
        prompt_tokens=cover.usage.prompt_tokens,
        completion_tokens=cover.usage.completion_tokens,
        total_tokens=cover.usage.total_tokens,
        estimated_cost_usd=cover.usage.estimated_cost_usd,
        details={"resume_loaded": bool(resume_text)},
    )
    _track_event(
        conn,
        user_id=user_id,
        chat_id=chat_id,
        offer_slug=offer.slug,
        event_type="cover_generated",
        details={"lead_id": _safe(lead.get("lead_id"))},
    )
    intro = [
        "✉️ Tailored cover note",
        f"{_safe(lead.get('title'))}",
        f"Company: {_safe(lead.get('company')) or '-'}",
        f"Pack: {_offer_title(offer)}",
        f"Stack: {_selected_stack_label(conn, user_id=user_id, offer=offer)}",
        f"Resume: {'CV loaded' if resume_text else 'No CV, using pack and stack only'}",
        "",
        "Draft",
        "",
        _format_cover_body(cover.text),
        "",
        "Next step",
        "Review it, adjust if needed, then send it manually on the platform.",
    ]
    api.send_message(
        chat_id=chat_id,
        text="\n".join(intro),
        reply_markup=_apply_keyboard(lead=lead),
    )


def _send_plan_invoice(
    api: TelegramBotApi,
    conn,
    settings: BotSettings,
    *,
    offer: OfferProfile,
    user_id: int,
    chat_id: int,
    plan: Dict[str, Any],
) -> None:
    code = _safe(plan.get("code")).lower()
    days = int(plan.get("days") or 0)
    stars = int(plan.get("stars") or 0)
    payload = f"offer={offer.slug}|plan={code}|days={days}|kind=one_time"
    api.send_invoice(
        chat_id=chat_id,
        title=f"{_offer_title(offer)} - {_safe(plan.get('title'))}",
        description=_safe(plan.get("description")) or f"Access to {_offer_title(offer)}",
        payload=payload,
        amount_stars=stars,
        label=_safe(plan.get("title")) or code,
        start_parameter=f"{offer.slug}-{code}",
        photo_url=settings.photo_url,
    )
    _track_event(
        conn,
        user_id=user_id,
        chat_id=chat_id,
        offer_slug=offer.slug,
        event_type="invoice_sent",
        status="ok",
        details={"plan_code": code, "days": days, "stars": stars},
    )


def _handle_successful_payment(api: TelegramBotApi, conn, settings: BotSettings, *, message: Dict[str, Any]) -> None:
    payment = dict(message.get("successful_payment") or {})
    payload = _parse_payload(_safe(payment.get("invoice_payload")))
    offer = _resolve_offer(settings, _safe(payload.get("offer")) or settings.default_offer_slug)
    user = dict(message.get("from") or {})
    chat = dict(message.get("chat") or {})
    document = dict(message.get("document") or {})
    user_id = int(user.get("id") or 0)
    chat_id = int(chat.get("id") or 0)
    username = _safe(user.get("username"))
    first_name = _safe(user.get("first_name"))
    upsert_bot_user(
        conn,
        BotUser(
            user_id=user_id,
            chat_id=chat_id,
            username=username,
            first_name=_safe(user.get("first_name")),
            last_name=_safe(user.get("last_name")),
        ),
    )
    set_user_selected_offer(conn, user_id=user_id, offer_slug=offer.slug)
    access = add_payment_and_grant_access(
        conn,
        user_id=user_id,
        offer_slug=offer.slug,
        plan_code=_safe(payload.get("plan")) or "unknown",
        days=max(1, int(payload.get("days") or 1)),
        charge_id=_safe(payment.get("telegram_payment_charge_id")),
        invoice_payload=_safe(payment.get("invoice_payload")),
        currency=_safe(payment.get("currency")) or "XTR",
        total_amount=int(payment.get("total_amount") or 0),
        is_recurring=False,
        raw_payment=payment,
    )
    _track_event(
        conn,
        user_id=user_id,
        chat_id=chat_id,
        offer_slug=offer.slug,
        event_type="payment_success",
        status="ok",
        details={
            "plan_code": _safe(payload.get("plan")) or "unknown",
            "days": max(1, int(payload.get("days") or 1)),
            "total_amount": int(payment.get("total_amount") or 0),
        },
    )
    conn.commit()
    api.send_message(
        chat_id=chat_id,
        text=(
            "Payment received.\n"
            f"Pack: {_offer_title(offer)}\n"
            f"Access unlocked until: {access.get('ends_at')}\n"
            "Sending your full shortlist now."
        ),
    )
    _send_feed(
        api=api,
        conn=conn,
        settings=settings,
        offer=offer,
        user_id=user_id,
        username=_safe(user.get("username")),
        chat_id=chat_id,
        limit=_offer_full_limit(offer),
        delivery_kind="paid_full",
    )
    conn.commit()


def _handle_pre_checkout(api: TelegramBotApi, conn, settings: BotSettings, *, pre_checkout_query: Dict[str, Any]) -> None:
    payload = _parse_payload(_safe(pre_checkout_query.get("invoice_payload")))
    offer = _resolve_offer(settings, _safe(payload.get("offer")) or settings.default_offer_slug)
    plan = _plan_map(offer).get(_safe(payload.get("plan")).lower())
    from_user = dict(pre_checkout_query.get("from") or {})
    user_id = int(from_user.get("id") or 0)
    ok = bool(plan)
    error_message = ""
    if not plan:
        ok = False
        error_message = "Plan is unavailable. Please reopen /plans and try again."
    elif int(pre_checkout_query.get("total_amount") or 0) != int(plan.get("stars") or 0):
        ok = False
        error_message = "Price mismatch. Please reopen /plans and try again."
    _track_event(
        conn,
        user_id=user_id,
        chat_id=user_id,
        offer_slug=offer.slug,
        event_type="pre_checkout",
        status="ok" if ok else "fail",
        details={"plan_code": _safe(payload.get("plan")), "total_amount": int(pre_checkout_query.get("total_amount") or 0)},
    )
    api.answer_pre_checkout_query(
        pre_checkout_query_id=_safe(pre_checkout_query.get("id")),
        ok=ok,
        error_message=error_message,
    )


def _handle_callback(api: TelegramBotApi, conn, settings: BotSettings, *, callback_query: Dict[str, Any]) -> None:
    data = _safe(callback_query.get("data"))
    from_user = dict(callback_query.get("from") or {})
    msg = dict(callback_query.get("message") or {})
    chat = dict(msg.get("chat") or {})
    user_id = int(from_user.get("id") or 0)
    chat_id = int(chat.get("id") or 0)
    username = _safe(from_user.get("username"))
    upsert_bot_user(
        conn,
        BotUser(
            user_id=user_id,
            chat_id=chat_id,
            username=username,
            first_name=_safe(from_user.get("first_name")),
            last_name=_safe(from_user.get("last_name")),
        ),
    )

    current_offer = _current_offer(conn, settings, user_id=user_id)

    if data == "preview":
        _track_event(conn, user_id=user_id, chat_id=chat_id, offer_slug=current_offer.slug, event_type="preview_requested")
        api.answer_callback_query(callback_query_id=_safe(callback_query.get("id")), text="Sending preview")
        _send_preview_with_pitch(api, conn, settings, offer=current_offer, user_id=user_id, username=username, chat_id=chat_id)
    elif data == "today":
        _track_event(conn, user_id=user_id, chat_id=chat_id, offer_slug=current_offer.slug, event_type="today_requested")
        has_access = _has_offer_access(settings, conn, user_id=user_id, username=username, offer_slug=current_offer.slug)
        if has_access:
            api.answer_callback_query(callback_query_id=_safe(callback_query.get("id")), text="Sending full shortlist")
            _send_feed(
                api=api,
                conn=conn,
                settings=settings,
                offer=current_offer,
                user_id=user_id,
                username=username,
                chat_id=chat_id,
                limit=_offer_full_limit(current_offer),
                delivery_kind="member_full",
            )
        else:
            api.answer_callback_query(callback_query_id=_safe(callback_query.get("id")), text="Preview only", show_alert=False)
            _send_preview_with_pitch(api, conn, settings, offer=current_offer, user_id=user_id, username=username, chat_id=chat_id)
    elif data == "plans":
        _track_event(conn, user_id=user_id, chat_id=chat_id, offer_slug=current_offer.slug, event_type="plans_opened")
        api.answer_callback_query(callback_query_id=_safe(callback_query.get("id")), text="Opening plans")
        api.send_message(
            chat_id=chat_id,
            text=f"Choose a plan for {_offer_title(current_offer)}.",
            reply_markup=_build_plans_keyboard(settings, offer=current_offer),
        )
    elif data == "choose_menu":
        _track_event(conn, user_id=user_id, chat_id=chat_id, offer_slug=current_offer.slug, event_type="choose_opened")
        api.answer_callback_query(callback_query_id=_safe(callback_query.get("id")), text="Choose profession")
        _send_offer_picker(api, settings, chat_id=chat_id, current_offer=current_offer)
    elif data == "stack_menu":
        _track_event(conn, user_id=user_id, chat_id=chat_id, offer_slug=current_offer.slug, event_type="stack_opened")
        api.answer_callback_query(callback_query_id=_safe(callback_query.get("id")), text="Choose stack")
        _send_stack_picker(api, conn, settings, chat_id=chat_id, user_id=user_id, current_offer=current_offer, username=username)
    elif data == "cv_menu":
        _track_event(conn, user_id=user_id, chat_id=chat_id, offer_slug=current_offer.slug, event_type="cv_prompt_opened")
        api.answer_callback_query(callback_query_id=_safe(callback_query.get("id")), text="Send your CV")
        _send_cv_prompt(
            api,
            conn,
            settings,
            chat_id=chat_id,
            user_id=user_id,
            username=username,
            first_name=_safe(from_user.get("first_name")),
            current_offer=current_offer,
        )
    elif data == "sources":
        _track_event(conn, user_id=user_id, chat_id=chat_id, offer_slug=current_offer.slug, event_type="sources_opened")
        api.answer_callback_query(callback_query_id=_safe(callback_query.get("id")), text="Opening sources")
        _send_sources(api, conn, settings, offer=current_offer, user_id=user_id, username=username, chat_id=chat_id)
    elif data.startswith("cover:"):
        lead_id = data.split(":", 1)[1].strip()
        _track_event(conn, user_id=user_id, chat_id=chat_id, offer_slug=current_offer.slug, event_type="cover_requested", details={"lead_id": lead_id})
        has_access = _has_offer_access(settings, conn, user_id=user_id, username=username, offer_slug=current_offer.slug)
        api.answer_callback_query(
            callback_query_id=_safe(callback_query.get("id")),
            text="Generating cover" if has_access else "Paid feature",
        )
        _send_cover_for_lead(
            api,
            conn,
            settings,
            offer=current_offer,
            user_id=user_id,
            username=username,
            chat_id=chat_id,
            lead_id=lead_id,
        )
    elif data.startswith("apply_idx:"):
        raw_index = data.split(":", 1)[1].strip()
        try:
            lead_index = int(raw_index or "0")
        except Exception:
            lead_index = 0
        _track_event(
            conn,
            user_id=user_id,
            chat_id=chat_id,
            offer_slug=current_offer.slug,
            event_type="apply_requested",
            details={"lead_index": lead_index, "origin": "callback"},
        )
        api.answer_callback_query(
            callback_query_id=_safe(callback_query.get("id")),
            text=f"Analyze #{lead_index}" if lead_index > 0 else "Pick a valid lead",
        )
        if lead_index <= 0:
            api.send_message(chat_id=chat_id, text="That lead number is invalid. Open Today again and pick a valid lead.")
        else:
            _send_apply_package(
                api,
                conn,
                settings,
                offer=current_offer,
                user_id=user_id,
                username=username,
                chat_id=chat_id,
                lead_index=lead_index,
            )
    elif data.startswith("choose:"):
        chosen_slug = data.split(":", 1)[1].strip()
        chosen_offer = settings.offers.get(chosen_slug)
        selectable = bool(_offer_bot_cfg(chosen_offer).get("selectable", True)) if chosen_offer is not None else False
        if chosen_offer is None or not selectable:
            api.answer_callback_query(callback_query_id=_safe(callback_query.get("id")), text="Unknown pack", show_alert=True)
        else:
            set_user_selected_offer(conn, user_id=user_id, offer_slug=chosen_offer.slug)
            _track_event(
                conn,
                user_id=user_id,
                chat_id=chat_id,
                offer_slug=chosen_offer.slug,
                event_type="offer_selected",
                details={"from_offer": current_offer.slug},
            )
            conn.commit()
            api.answer_callback_query(callback_query_id=_safe(callback_query.get("id")), text=f"Switched to {_offer_title(chosen_offer)}")
            api.send_message(
                chat_id=chat_id,
                text=(
                    f"Current pack: {_offer_title(chosen_offer)}\n"
                    f"Current stack: {_selected_stack_label(conn, user_id=user_id, offer=chosen_offer)}\n"
                    f"{_offer_summary(chosen_offer)}\n"
                    "Use Stack, Preview, Today, or Plans."
                ),
                reply_markup=_main_menu_keyboard(
                    settings,
                    offer=chosen_offer,
                    has_access=_has_offer_access(settings, conn, user_id=user_id, username=username, offer_slug=chosen_offer.slug),
                ),
            )
    elif data.startswith("stack:"):
        chosen_code = data.split(":", 1)[1].strip().lower()
        stack_map = _stack_option_map(current_offer)
        if chosen_code and chosen_code != "any" and chosen_code not in stack_map:
            api.answer_callback_query(callback_query_id=_safe(callback_query.get("id")), text="Unknown stack", show_alert=True)
        else:
            set_user_selected_stack(conn, user_id=user_id, offer_slug=current_offer.slug, stack_code="" if chosen_code == "any" else chosen_code)
            _track_event(
                conn,
                user_id=user_id,
                chat_id=chat_id,
                offer_slug=current_offer.slug,
                event_type="stack_selected",
                details={"stack_code": "" if chosen_code == "any" else chosen_code},
            )
            conn.commit()
            api.answer_callback_query(
                callback_query_id=_safe(callback_query.get("id")),
                text=f"Stack: {_selected_stack_label(conn, user_id=user_id, offer=current_offer)}",
            )
            api.send_message(
                chat_id=chat_id,
                text=(
                    f"Current pack: {_offer_title(current_offer)}\n"
                    f"Current stack: {_selected_stack_label(conn, user_id=user_id, offer=current_offer)}\n"
                    "Today's shortlist will now prioritize this stack."
                ),
                reply_markup=_main_menu_keyboard(
                    settings,
                    offer=current_offer,
                    has_access=_has_offer_access(settings, conn, user_id=user_id, username=username, offer_slug=current_offer.slug),
                ),
            )
    elif data.startswith("buy:"):
        parts = data.split(":", 2)
        chosen_slug = parts[1].strip() if len(parts) >= 2 else current_offer.slug
        code = parts[2].strip().lower() if len(parts) >= 3 else ""
        offer = _resolve_offer(settings, chosen_slug)
        plan = _plan_map(offer).get(code)
        if not plan:
            api.answer_callback_query(callback_query_id=_safe(callback_query.get("id")), text="Unknown plan", show_alert=True)
        else:
            set_user_selected_offer(conn, user_id=user_id, offer_slug=offer.slug)
            _track_event(
                conn,
                user_id=user_id,
                chat_id=chat_id,
                offer_slug=offer.slug,
                event_type="buy_clicked",
                details={"plan_code": code},
            )
            conn.commit()
            api.answer_callback_query(callback_query_id=_safe(callback_query.get("id")), text="Opening payment")
            _send_plan_invoice(api, conn, settings, offer=offer, user_id=user_id, chat_id=chat_id, plan=plan)
    else:
        api.answer_callback_query(callback_query_id=_safe(callback_query.get("id")), text="Unsupported action")
    conn.commit()


def _handle_message(api: TelegramBotApi, conn, settings: BotSettings, *, message: Dict[str, Any]) -> None:
    if message.get("successful_payment"):
        _handle_successful_payment(api, conn, settings, message=message)
        return

    user = dict(message.get("from") or {})
    chat = dict(message.get("chat") or {})
    document = dict(message.get("document") or {})
    user_id = int(user.get("id") or 0)
    chat_id = int(chat.get("id") or 0)
    username = _safe(user.get("username"))
    first_name = _safe(user.get("first_name"))
    upsert_bot_user(
        conn,
        BotUser(
            user_id=user_id,
            chat_id=chat_id,
            username=username,
            first_name=_safe(user.get("first_name")),
            last_name=_safe(user.get("last_name")),
        ),
    )
    current_offer = _current_offer(conn, settings, user_id=user_id)
    text = _safe(message.get("text") or message.get("caption"))
    command = _parse_command(text)
    session = _resume_session(user_id)
    resume_document = bool(document) and (
        session.awaiting_text or (_looks_like_resume_document(document) and not command)
    )

    if session.awaiting_text and text and not command:
        _store_transient_resume(user_id=user_id, resume_text=text)
        _track_event(
            conn,
            user_id=user_id,
            chat_id=chat_id,
            offer_slug=current_offer.slug,
            event_type="resume_loaded",
            details={"chars": len(_current_resume_text(user_id=user_id))},
        )
        api.send_message(
            chat_id=chat_id,
            text=(
                f"{_display_name(username=username, first_name=first_name)}, temporary resume loaded.\n"
                "It stays only in bot memory for a short time and is not written to our DB or long-term files.\n"
                "Now run /apply 1 after /today, or /forgetcv to clear it."
            ),
            reply_markup=_main_menu_keyboard(
                settings,
                offer=current_offer,
                has_access=_has_offer_access(settings, conn, user_id=user_id, username=username, offer_slug=current_offer.slug),
            ),
        )
        conn.commit()
        return
    if resume_document:
        api.send_message(
            chat_id=chat_id,
            text=(
                f"{_display_name(username=username, first_name=first_name)}, resume file received.\n"
                "Reading it now. I will keep it only in temporary bot memory, not in our DB or long-term files."
            ),
        )
        try:
            resume_text = _extract_resume_text_from_document(api, document=document)
        except Exception as exc:
            api.send_message(
                chat_id=chat_id,
                text=f"I could not read this resume file: {_safe(exc)}",
            )
            conn.commit()
            return
        _store_transient_resume(user_id=user_id, resume_text=resume_text)
        _track_event(
            conn,
            user_id=user_id,
            chat_id=chat_id,
            offer_slug=current_offer.slug,
            event_type="resume_loaded",
            details={"chars": len(_current_resume_text(user_id=user_id)), "kind": "document"},
        )
        api.send_message(
            chat_id=chat_id,
            text=(
                f"{_display_name(username=username, first_name=first_name)}, resume accepted and parsed.\n"
                "It is now loaded in temporary bot memory only.\n"
                "Now run /apply 1 after /today, or /forgetcv to clear it."
            ),
            reply_markup=_main_menu_keyboard(
                settings,
                offer=current_offer,
                has_access=_has_offer_access(settings, conn, user_id=user_id, username=username, offer_slug=current_offer.slug),
            ),
        )
        conn.commit()
        return

    if command in ("/start", "/help"):
        _track_event(conn, user_id=user_id, chat_id=chat_id, offer_slug=current_offer.slug, event_type="start" if command == "/start" else "help")
        welcome = (
            f"{settings.bot_name}\n"
            f"Hi, {_display_name(username=username, first_name=first_name)}.\n"
            f"Current pack: {_offer_title(current_offer)}\n"
            f"Current stack: {_selected_stack_label(conn, user_id=user_id, offer=current_offer)}\n"
            f"{_resume_status_line(user_id=user_id)}\n"
            "I send filtered remote work leads, not generic chat.\n"
            "Use /choose to switch profession packs, /stack to focus the stack, tap CV to add your resume, /today for the shortlist, /apply 1 for AI analysis, or /plans to unlock full access."
        )
        has_access = _has_offer_access(settings, conn, user_id=user_id, username=username, offer_slug=current_offer.slug)
        api.send_message(
            chat_id=chat_id,
            text=welcome,
            reply_markup=_main_menu_keyboard(settings, offer=current_offer, has_access=has_access),
        )
    elif command in ("/choose", "/profession", "/professions"):
        _track_event(conn, user_id=user_id, chat_id=chat_id, offer_slug=current_offer.slug, event_type="choose_opened")
        _send_offer_picker(api, settings, chat_id=chat_id, current_offer=current_offer)
    elif command == "/stack":
        _track_event(conn, user_id=user_id, chat_id=chat_id, offer_slug=current_offer.slug, event_type="stack_opened")
        _send_stack_picker(api, conn, settings, chat_id=chat_id, user_id=user_id, current_offer=current_offer, username=username)
    elif command == "/sources":
        _track_event(conn, user_id=user_id, chat_id=chat_id, offer_slug=current_offer.slug, event_type="sources_opened")
        _send_sources(api, conn, settings, offer=current_offer, user_id=user_id, username=username, chat_id=chat_id)
    elif command == "/today":
        _track_event(conn, user_id=user_id, chat_id=chat_id, offer_slug=current_offer.slug, event_type="today_requested")
        has_access = _has_offer_access(settings, conn, user_id=user_id, username=username, offer_slug=current_offer.slug)
        if has_access:
            _send_feed(
                api=api,
                conn=conn,
                settings=settings,
                offer=current_offer,
                user_id=user_id,
                username=username,
                chat_id=chat_id,
                limit=_offer_full_limit(current_offer),
                delivery_kind="member_full",
            )
        else:
            _send_preview_with_pitch(api, conn, settings, offer=current_offer, user_id=user_id, username=username, chat_id=chat_id)
    elif command == "/apply":
        _track_event(conn, user_id=user_id, chat_id=chat_id, offer_slug=current_offer.slug, event_type="apply_requested")
        raw_arg = _command_arg(text)
        try:
            lead_index = int(raw_arg or "0")
        except Exception:
            lead_index = 0
        if lead_index <= 0:
            reply_text = _safe(dict(message.get("reply_to_message") or {}).get("text"))
            lead_index = _extract_lead_index_from_card_text(reply_text)
        if lead_index <= 0:
            api.send_message(
                chat_id=chat_id,
                text=(
                    "Pick a specific lead first.\n"
                    "Tap an Analyze button under a shortlist card, or use /apply N, for example /apply 5."
                ),
            )
        else:
            _send_apply_package(
                api,
                conn,
                settings,
                offer=current_offer,
                user_id=user_id,
                username=username,
                chat_id=chat_id,
                lead_index=lead_index,
            )
    elif command == "/cv":
        body = _command_arg(text)
        if document:
            api.send_message(
                chat_id=chat_id,
                text=(
                    f"{_display_name(username=username, first_name=first_name)}, resume file received.\n"
                    "Reading it now. I will keep it only in temporary bot memory, not in our DB or long-term files."
                ),
            )
            try:
                resume_text = _extract_resume_text_from_document(api, document=document)
            except Exception as exc:
                api.send_message(chat_id=chat_id, text=f"I could not read this resume file: {_safe(exc)}")
                conn.commit()
                return
            _store_transient_resume(user_id=user_id, resume_text=resume_text)
            _track_event(
                conn,
                user_id=user_id,
                chat_id=chat_id,
                offer_slug=current_offer.slug,
                event_type="resume_loaded",
                details={"chars": len(_current_resume_text(user_id=user_id)), "kind": "document"},
            )
            api.send_message(
                chat_id=chat_id,
                text=(
                    f"{_display_name(username=username, first_name=first_name)}, resume accepted and parsed.\n"
                    "It is now loaded in temporary bot memory only.\n"
                    "Now run /apply 1 after /today, or /forgetcv to clear it."
                ),
            )
        elif body:
            _store_transient_resume(user_id=user_id, resume_text=body)
            _track_event(
                conn,
                user_id=user_id,
                chat_id=chat_id,
                offer_slug=current_offer.slug,
                event_type="resume_loaded",
                details={"chars": len(_current_resume_text(user_id=user_id))},
            )
            api.send_message(
                chat_id=chat_id,
                text=(
                    f"{_display_name(username=username, first_name=first_name)}, temporary resume loaded.\n"
                    "It is kept only in memory for a short time and is not written to our DB or long-term files.\n"
                    "Use /apply 1 after /today, or /forgetcv to clear it."
                ),
            )
        else:
            _send_cv_prompt(
                api,
                conn,
                settings,
                chat_id=chat_id,
                user_id=user_id,
                username=username,
                first_name=first_name,
                current_offer=current_offer,
            )
    elif command == "/forgetcv":
        _clear_transient_resume(user_id=user_id)
        _track_event(conn, user_id=user_id, chat_id=chat_id, offer_slug=current_offer.slug, event_type="resume_cleared")
        api.send_message(chat_id=chat_id, text=f"{_display_name(username=username, first_name=first_name)}, temporary resume cleared from bot memory.")
    elif command in ("/plans", "/buy"):
        _track_event(conn, user_id=user_id, chat_id=chat_id, offer_slug=current_offer.slug, event_type="plans_opened")
        api.send_message(
            chat_id=chat_id,
            text=f"Choose a plan for {_offer_title(current_offer)}.",
            reply_markup=_build_plans_keyboard(settings, offer=current_offer),
        )
    elif command == "/status":
        _track_event(conn, user_id=user_id, chat_id=chat_id, offer_slug=current_offer.slug, event_type="status_viewed")
        _send_status(api, conn, settings, offer=current_offer, user_id=user_id, username=username, chat_id=chat_id)
    elif command in ("/adminstats", "/admin", "/stats_admin"):
        if _is_admin(settings, user_id=user_id, chat_id=chat_id, username=username):
            _track_event(conn, user_id=user_id, chat_id=chat_id, offer_slug=current_offer.slug, event_type="admin_stats_viewed")
            api.send_message(chat_id=chat_id, text=_format_admin_stats(settings, get_bot_analytics_summary(conn)))
        else:
            api.send_message(chat_id=chat_id, text="Admin stats are not available for this account.")
    elif command == "/terms":
        api.send_message(chat_id=chat_id, text=_build_terms_text(settings))
    elif command in ("/support", "/paysupport"):
        api.send_message(chat_id=chat_id, text=_build_support_text(settings))
    else:
        if document:
            api.send_message(
                chat_id=chat_id,
                text=(
                    "I received a file, but I do not know what to do with it yet.\n"
                    "Tap CV first, then send a PDF, TXT, or MD resume, or upload a file with cv/resume in its name."
                ),
                reply_markup=_main_menu_keyboard(
                    settings,
                    offer=current_offer,
                    has_access=_has_offer_access(settings, conn, user_id=user_id, username=username, offer_slug=current_offer.slug),
                ),
            )
            conn.commit()
            return
        api.send_message(
            chat_id=chat_id,
            text="Unknown command. Try /choose, /stack, /sources, /today, /apply, /cv, /forgetcv, /plans, /status, /terms, or /support.",
            reply_markup=_main_menu_keyboard(
                settings,
                offer=current_offer,
                has_access=_has_offer_access(settings, conn, user_id=user_id, username=username, offer_slug=current_offer.slug),
            ),
        )
    conn.commit()


def _load_settings(args) -> BotSettings:
    load_env_file(ROOT / ".env.accounts")
    load_env_file(ROOT / ".env")

    cfg_path = resolve_path(ROOT, args.config)
    cfg = load_config(str(cfg_path)) if cfg_path.exists() else {}
    db_path = resolve_path(ROOT, str(cfg_get(cfg, "activity.db_path", "data/out/activity.sqlite")))
    if _safe(args.db):
        db_path = resolve_path(ROOT, _safe(args.db))

    offers_path = resolve_path(ROOT, args.offers)
    offers = load_offer_profiles(offers_path)
    default_offer_slug = _safe(args.offer) or _safe(os.getenv("TELEGRAM_BOT_OFFER")) or "qa_gig_hunter"
    if default_offer_slug not in offers:
        raise SystemExit(f"Unknown TELEGRAM_BOT_OFFER/default offer: {default_offer_slug}")
    default_offer = offers[default_offer_slug]
    bot_cfg = _offer_bot_cfg(default_offer)

    token = _safe(os.getenv("TELEGRAM_BOT_TOKEN"))
    if not token:
        raise SystemExit("Missing TELEGRAM_BOT_TOKEN.")

    support_handle = _safe(os.getenv("TELEGRAM_SUPPORT_HANDLE"))
    support_text = _safe(os.getenv("TELEGRAM_SUPPORT_TEXT"))
    if support_handle and not support_text:
        support_text = f"Support: {support_handle}"
    terms_url = _safe(os.getenv("TELEGRAM_TERMS_URL"))
    terms_text = _safe(os.getenv("TELEGRAM_TERMS_TEXT"))
    if terms_url and not terms_text:
        terms_text = f"Terms: {terms_url}"
    free_user_ids = _split_ints(os.getenv("TELEGRAM_FREE_USER_IDS") or "")
    free_usernames = _split_names(os.getenv("TELEGRAM_FREE_USERNAMES") or "")
    admin_user_ids = _split_ints(os.getenv("TELEGRAM_ADMIN_USER_IDS") or "")
    admin_usernames = _split_names(os.getenv("TELEGRAM_ADMIN_USERNAMES") or "")

    return BotSettings(
        token=token,
        default_offer_slug=default_offer.slug,
        bot_name=_safe(bot_cfg.get("display_name")) or default_offer.title,
        db_path=db_path,
        offers_path=offers_path,
        offers=offers,
        support_text=support_text,
        terms_text=terms_text,
        admin_chat_id=_int_env("TELEGRAM_ADMIN_CHAT_ID", 0),
        poll_timeout=max(10, int(args.poll_timeout)),
        sleep_sec=max(0.2, float(args.sleep_sec)),
        photo_url=_safe(os.getenv("TELEGRAM_BOT_PHOTO_URL")),
        commands=list(bot_cfg.get("commands") or []),
        free_user_ids=free_user_ids,
        free_usernames=free_usernames,
        admin_user_ids=admin_user_ids,
        admin_usernames=admin_usernames,
    )


def main() -> int:
    ap = argparse.ArgumentParser(description="Telegram paid bot MVP for AIJobSearcher offers.")
    ap.add_argument("--config", default="config/config.yaml")
    ap.add_argument("--offers", default="config/offers.yaml")
    ap.add_argument("--offer", default="")
    ap.add_argument("--db", default="")
    ap.add_argument("--poll-timeout", type=int, default=25)
    ap.add_argument("--sleep-sec", type=float, default=1.5)
    args = ap.parse_args()

    settings = _load_settings(args)
    api = TelegramBotApi(token=settings.token, timeout_sec=max(15, settings.poll_timeout + 5))
    me = api.get_me()
    print(f"[tg-paid-bot] bot=@{_safe(me.get('username'))} default_offer={settings.default_offer_slug}")
    try:
        api.delete_webhook(drop_pending_updates=False)
        print("[tg-paid-bot] webhook cleared for polling mode")
    except TelegramApiError as e:
        print(f"[tg-paid-bot] deleteWebhook skipped: {e}")

    conn = db_connect(settings.db_path)
    init_db(conn)
    offset = 0
    allowed_updates = ["message", "callback_query", "pre_checkout_query"]

    while True:
        try:
            updates = api.get_updates(offset=offset, timeout=settings.poll_timeout, allowed_updates=allowed_updates)
            for upd in updates:
                next_offset = int(upd.get("update_id") or 0) + 1
                if upd.get("pre_checkout_query"):
                    _handle_pre_checkout(api, conn, settings, pre_checkout_query=dict(upd.get("pre_checkout_query") or {}))
                    offset = next_offset
                    continue
                if upd.get("callback_query"):
                    _handle_callback(api, conn, settings, callback_query=dict(upd.get("callback_query") or {}))
                    offset = next_offset
                    continue
                if upd.get("message"):
                    _handle_message(api, conn, settings, message=dict(upd.get("message") or {}))
                    offset = next_offset
                    continue
        except KeyboardInterrupt:
            print("[tg-paid-bot] interrupted")
            break
        except TelegramApiError as e:
            print(f"[tg-paid-bot] telegram api error: {e}")
            try:
                conn.rollback()
            except Exception:
                pass
            err_text = _safe(e).lower()
            if "409" in err_text or "conflict:" in err_text:
                print(
                    "[tg-paid-bot] polling conflict: another bot instance is using the same token. "
                    "Stop any local/duplicate poller or rotate the token if the conflict persists."
                )
                try:
                    api.delete_webhook(drop_pending_updates=False)
                except Exception:
                    pass
                time.sleep(max(10.0, settings.sleep_sec * 4))
                continue
            time.sleep(max(3.0, settings.sleep_sec))
        except Exception as e:
            print(f"[tg-paid-bot] error: {e}")
            try:
                conn.rollback()
            except Exception:
                pass
            time.sleep(max(3.0, settings.sleep_sec))

    try:
        conn.close()
    except Exception:
        pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
