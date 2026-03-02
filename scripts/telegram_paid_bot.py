from __future__ import annotations

import argparse
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Set

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.activity_db import connect as db_connect, init_db  # noqa: E402
from src.config import cfg_get, load_config, resolve_path  # noqa: E402
from src.email_sender import load_env_file  # noqa: E402
from src.offer_feed import build_offer_rows  # noqa: E402
from src.offer_profiles import OfferProfile, load_offer_profiles  # noqa: E402
from src.telegram_bot_api import TelegramApiError, TelegramBotApi  # noqa: E402
from src.telegram_paid_store import (  # noqa: E402
    BotUser,
    add_payment_and_grant_access,
    get_bot_analytics_summary,
    get_active_subscription,
    get_user_selected_offer,
    get_user_summary,
    log_bot_event,
    log_delivery,
    set_user_selected_offer,
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


def _parse_command(text: str) -> str:
    cmd = _safe(text).split()[0].strip()
    if "@" in cmd:
        cmd = cmd.split("@", 1)[0]
    return cmd.lower()


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


def _build_offer_selector_keyboard(settings: BotSettings, *, current_offer_slug: str) -> Dict[str, Any]:
    rows: List[List[Dict[str, str]]] = []
    for offer in _selectable_offers(settings):
        slug = offer.slug
        title = _offer_title(offer)
        prefix = "✅ " if slug == current_offer_slug else ""
        rows.append([{"text": f"{prefix}{_offer_emoji(offer)} {title}", "callback_data": f"choose:{slug}"}])
    rows.append([{"text": "💳 Plans", "callback_data": "plans"}, {"text": "📦 Today", "callback_data": "today"}])
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
    rows.append([{"text": "🎯 Choose profession", "callback_data": "choose_menu"}, {"text": "👀 Preview", "callback_data": "preview"}])
    rows.append([{"text": "📦 Today", "callback_data": "today"}])
    return {"inline_keyboard": rows}


def _main_menu_keyboard(settings: BotSettings, *, offer: OfferProfile, has_access: bool) -> Dict[str, Any]:
    rows: List[List[Dict[str, str]]] = [
        [{"text": "🎯 Choose profession", "callback_data": "choose_menu"}],
        [{"text": "👀 Preview", "callback_data": "preview"}, {"text": "💳 Plans", "callback_data": "plans"}],
    ]
    if has_access:
        rows.append([{"text": "📦 Today's shortlist", "callback_data": "today"}])
    else:
        rows.append([{"text": "🔓 Unlock full shortlist", "callback_data": "plans"}])
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


def _format_card(row: Dict[str, Any], *, index: int) -> str:
    stack_hits = [str(x).strip() for x in row.get("stack_hits") or [] if str(x).strip()]
    lines = [
        f"{index}. {_safe(row.get('title'))}",
        f"Platform: {_safe(row.get('platform')) or '-'}",
        f"Type: {_safe(row.get('lead_type')) or '-'}",
        f"Location: {_safe(row.get('location')) or 'Remote/unspecified'}",
        f"Contact: {_safe(row.get('contact_method')) or '-'}",
        f"Score: {_safe(row.get('score')) or '-'}",
    ]
    if stack_hits:
        lines.append(f"Stack: {', '.join(stack_hits[:6])}")
    lines.extend(
        [
        f"Why it fits: {_why_selected(row)}",
        f"Link: {_safe(row.get('url'))}",
        ]
    )
    return "\n".join(lines)


def _is_free_user(settings: BotSettings, *, user_id: int, username: str) -> bool:
    if int(user_id or 0) in settings.free_user_ids:
        return True
    uname = _safe(username).lstrip("@").lower()
    return bool(uname) and uname in settings.free_usernames


def _has_offer_access(settings: BotSettings, conn, *, user_id: int, username: str, offer_slug: str) -> bool:
    if _is_free_user(settings, user_id=user_id, username=username):
        return True
    return get_active_subscription(conn, user_id=user_id, offer_slug=offer_slug) is not None


def _is_admin(settings: BotSettings, *, user_id: int, chat_id: int, username: str) -> bool:
    admin_id = int(settings.admin_chat_id or 0)
    if admin_id and (int(user_id or 0) == admin_id or int(chat_id or 0) == admin_id):
        return True
    return _is_free_user(settings, user_id=user_id, username=username)


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

    return "\n".join(lines)


def _chunk_text(rows: List[Dict[str, Any]], *, chunk_size: int) -> List[str]:
    out: List[str] = []
    size = max(1, int(chunk_size))
    for start in range(0, len(rows), size):
        block = rows[start:start + size]
        parts = [_format_card(row, index=start + idx + 1) for idx, row in enumerate(block)]
        out.append("\n\n".join(parts))
    return out


def _delivery_label(delivery_kind: str) -> str:
    mapping = {
        "preview": "preview",
        "member_full": "full access",
        "paid_full": "full access",
    }
    return mapping.get(_safe(delivery_kind), _safe(delivery_kind))


def _build_terms_text(settings: BotSettings) -> str:
    if settings.terms_text:
        return settings.terms_text
    return (
        "Terms\n"
        "- This bot sells access to filtered job/gig leads, not guaranteed employment.\n"
        "- Refund handling is manual via support.\n"
        "- Access duration starts after successful Telegram Stars payment.\n"
        "- Links and availability may change on source platforms."
    )


def _build_support_text(settings: BotSettings) -> str:
    if settings.support_text:
        return settings.support_text
    return "Support is not configured yet. Set TELEGRAM_SUPPORT_TEXT or TELEGRAM_SUPPORT_HANDLE."


def _get_rows(settings: BotSettings, *, offer: OfferProfile, limit: int) -> List[Dict[str, Any]]:
    conn = db_connect(settings.db_path)
    try:
        init_db(conn)
        return build_offer_rows(conn, offer=offer, scan_limit=max(500, limit * 4), limit=limit)
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
    rows = _get_rows(settings, offer=offer, limit=limit)
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

    header = (
        f"{_offer_title(offer)}\n"
        f"Leads in this drop: {len(rows)}\n"
        f"Mode: {_delivery_label(delivery_kind)}"
    )
    has_access = _has_offer_access(settings, conn, user_id=user_id, username=username, offer_slug=offer.slug)
    first = api.send_message(
        chat_id=chat_id,
        text=header,
        reply_markup=_main_menu_keyboard(settings, offer=offer, has_access=has_access),
    )
    sent = 1
    chunk_size = 3 if delivery_kind == "preview" else 4
    for chunk in _chunk_text(rows, chunk_size=chunk_size):
        api.send_message(chat_id=chat_id, text=chunk)
        sent += 1
    log_delivery(
        conn,
        user_id=user_id,
        offer_slug=offer.slug,
        delivery_kind=delivery_kind,
        item_count=len(rows),
        message_id=first.get("message_id"),
        details={"messages_sent": sent},
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
    if _is_free_user(settings, user_id=user_id, username=username):
        text = (
            f"Current pack: {_offer_title(offer)}\n"
            "Status: free test access\n"
            f"Payments: {summary.get('payments_count')}\n"
            f"Deliveries: {summary.get('deliveries_count')}"
        )
        has_access = True
    elif active:
        text = (
            f"Current pack: {_offer_title(offer)}\n"
            "Status: active\n"
            f"Plan: {_safe(active.get('plan_code'))}\n"
            f"Access until: {_safe(active.get('ends_at'))}\n"
            f"Payments: {summary.get('payments_count')}\n"
            f"Deliveries: {summary.get('deliveries_count')}"
        )
        has_access = True
    else:
        text = (
            f"Current pack: {_offer_title(offer)}\n"
            "Status: no active access\n"
            f"Payments: {summary.get('payments_count')}\n"
            f"Deliveries: {summary.get('deliveries_count')}"
        )
        has_access = False
    api.send_message(chat_id=chat_id, text=text, reply_markup=_main_menu_keyboard(settings, offer=offer, has_access=has_access))


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
    user_id = int(user.get("id") or 0)
    chat_id = int(chat.get("id") or 0)
    username = _safe(user.get("username"))
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
                    f"{_offer_summary(chosen_offer)}\n"
                    "Use Preview, Today, or Plans."
                ),
                reply_markup=_main_menu_keyboard(
                    settings,
                    offer=chosen_offer,
                    has_access=_has_offer_access(settings, conn, user_id=user_id, username=username, offer_slug=chosen_offer.slug),
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
    user_id = int(user.get("id") or 0)
    chat_id = int(chat.get("id") or 0)
    username = _safe(user.get("username"))
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
    text = _safe(message.get("text"))
    command = _parse_command(text)

    if command in ("/start", "/help"):
        _track_event(conn, user_id=user_id, chat_id=chat_id, offer_slug=current_offer.slug, event_type="start" if command == "/start" else "help")
        welcome = (
            f"{settings.bot_name}\n"
            f"Current pack: {_offer_title(current_offer)}\n"
            "I send filtered remote work leads, not generic chat.\n"
            "Use /choose to switch profession packs, /today for the shortlist, or /plans to unlock full access."
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
        api.send_message(
            chat_id=chat_id,
            text="Unknown command. Try /choose, /today, /plans, /status, /terms, or /support.",
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

    conn = db_connect(settings.db_path)
    init_db(conn)
    offset = 0
    allowed_updates = ["message", "callback_query", "pre_checkout_query"]

    while True:
        try:
            updates = api.get_updates(offset=offset, timeout=settings.poll_timeout, allowed_updates=allowed_updates)
            for upd in updates:
                offset = int(upd.get("update_id") or 0) + 1
                if upd.get("pre_checkout_query"):
                    _handle_pre_checkout(api, conn, settings, pre_checkout_query=dict(upd.get("pre_checkout_query") or {}))
                    continue
                if upd.get("callback_query"):
                    _handle_callback(api, conn, settings, callback_query=dict(upd.get("callback_query") or {}))
                    continue
                if upd.get("message"):
                    _handle_message(api, conn, settings, message=dict(upd.get("message") or {}))
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
