from __future__ import annotations

import argparse
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List

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
    get_active_subscription,
    get_user_selected_offer,
    get_user_summary,
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
        prefix = "Current: " if slug == current_offer_slug else ""
        rows.append([{"text": f"{prefix}{title}", "callback_data": f"choose:{slug}"}])
    rows.append([{"text": "Plans", "callback_data": "plans"}, {"text": "Today", "callback_data": "today"}])
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
    rows.append([{"text": "Choose profession", "callback_data": "choose_menu"}, {"text": "Preview", "callback_data": "preview"}])
    rows.append([{"text": "Today", "callback_data": "today"}])
    return {"inline_keyboard": rows}


def _main_menu_keyboard(settings: BotSettings, *, offer: OfferProfile, has_access: bool) -> Dict[str, Any]:
    rows: List[List[Dict[str, str]]] = [
        [{"text": "Choose profession", "callback_data": "choose_menu"}],
        [{"text": "Preview", "callback_data": "preview"}, {"text": "Plans", "callback_data": "plans"}],
    ]
    if has_access:
        rows.append([{"text": "Today's shortlist", "callback_data": "today"}])
    else:
        rows.append([{"text": "Unlock full shortlist", "callback_data": "plans"}])
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
    lines = [
        f"{index}. {_safe(row.get('title'))}",
        f"Platform: {_safe(row.get('platform')) or '-'}",
        f"Type: {_safe(row.get('lead_type')) or '-'}",
        f"Location: {_safe(row.get('location')) or 'Remote/unspecified'}",
        f"Contact: {_safe(row.get('contact_method')) or '-'}",
        f"Score: {_safe(row.get('score')) or '-'}",
        f"Why it fits: {_why_selected(row)}",
        f"Link: {_safe(row.get('url'))}",
    ]
    return "\n".join(lines)


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
        "Choose the profession pack you want to hunt.",
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
        f"Mode: {delivery_kind}"
    )
    has_access = delivery_kind != "preview"
    first = api.send_message(
        chat_id=chat_id,
        text=header,
        reply_markup=_main_menu_keyboard(settings, offer=offer, has_access=has_access),
    )
    sent = 1
    for idx, row in enumerate(rows, start=1):
        api.send_message(chat_id=chat_id, text=_format_card(row, index=idx))
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
    chat_id: int,
) -> None:
    _send_feed(
        api=api,
        conn=conn,
        settings=settings,
        offer=offer,
        user_id=user_id,
        chat_id=chat_id,
        limit=_offer_preview_limit(offer),
        delivery_kind="preview",
    )
    pitch_lines = [
        f"{_offer_title(offer)}",
        "Unlock full access with Telegram Stars:",
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
    chat_id: int,
) -> None:
    summary = get_user_summary(conn, user_id=user_id, offer_slug=offer.slug)
    active = summary.get("active_subscription")
    if active:
        text = (
            f"Current pack: {_offer_title(offer)}\n"
            "Status: active\n"
            f"Plan: {_safe(active.get('plan_code'))}\n"
            f"Access until: {_safe(active.get('ends_at'))}\n"
            f"Payments: {summary.get('payments_count')}\n"
            f"Deliveries: {summary.get('deliveries_count')}"
        )
    else:
        text = (
            f"Current pack: {_offer_title(offer)}\n"
            "Status: no active access\n"
            f"Payments: {summary.get('payments_count')}\n"
            f"Deliveries: {summary.get('deliveries_count')}"
        )
    api.send_message(chat_id=chat_id, text=text, reply_markup=_main_menu_keyboard(settings, offer=offer, has_access=bool(active)))


def _send_plan_invoice(
    api: TelegramBotApi,
    settings: BotSettings,
    *,
    offer: OfferProfile,
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


def _handle_successful_payment(api: TelegramBotApi, conn, settings: BotSettings, *, message: Dict[str, Any]) -> None:
    payment = dict(message.get("successful_payment") or {})
    payload = _parse_payload(_safe(payment.get("invoice_payload")))
    offer = _resolve_offer(settings, _safe(payload.get("offer")) or settings.default_offer_slug)
    user = dict(message.get("from") or {})
    chat = dict(message.get("chat") or {})
    user_id = int(user.get("id") or 0)
    chat_id = int(chat.get("id") or 0)
    upsert_bot_user(
        conn,
        BotUser(
            user_id=user_id,
            chat_id=chat_id,
            username=_safe(user.get("username")),
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
        chat_id=chat_id,
        limit=_offer_full_limit(offer),
        delivery_kind="paid_full",
    )
    conn.commit()


def _handle_pre_checkout(api: TelegramBotApi, settings: BotSettings, *, pre_checkout_query: Dict[str, Any]) -> None:
    payload = _parse_payload(_safe(pre_checkout_query.get("invoice_payload")))
    offer = _resolve_offer(settings, _safe(payload.get("offer")) or settings.default_offer_slug)
    plan = _plan_map(offer).get(_safe(payload.get("plan")).lower())
    ok = bool(plan)
    error_message = ""
    if not plan:
        error_message = "Plan is unavailable. Please reopen /plans and try again."
    elif int(pre_checkout_query.get("total_amount") or 0) != int(plan.get("stars") or 0):
        ok = False
        error_message = "Price mismatch. Please reopen /plans and try again."
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
    upsert_bot_user(
        conn,
        BotUser(
            user_id=user_id,
            chat_id=chat_id,
            username=_safe(from_user.get("username")),
            first_name=_safe(from_user.get("first_name")),
            last_name=_safe(from_user.get("last_name")),
        ),
    )

    current_offer = _current_offer(conn, settings, user_id=user_id)

    if data == "preview":
        api.answer_callback_query(callback_query_id=_safe(callback_query.get("id")), text="Sending preview")
        _send_preview_with_pitch(api, conn, settings, offer=current_offer, user_id=user_id, chat_id=chat_id)
    elif data == "today":
        active = get_active_subscription(conn, user_id=user_id, offer_slug=current_offer.slug)
        if active:
            api.answer_callback_query(callback_query_id=_safe(callback_query.get("id")), text="Sending full shortlist")
            _send_feed(
                api=api,
                conn=conn,
                settings=settings,
                offer=current_offer,
                user_id=user_id,
                chat_id=chat_id,
                limit=_offer_full_limit(current_offer),
                delivery_kind="member_full",
            )
        else:
            api.answer_callback_query(callback_query_id=_safe(callback_query.get("id")), text="Preview only", show_alert=False)
            _send_preview_with_pitch(api, conn, settings, offer=current_offer, user_id=user_id, chat_id=chat_id)
    elif data == "plans":
        api.answer_callback_query(callback_query_id=_safe(callback_query.get("id")), text="Opening plans")
        api.send_message(
            chat_id=chat_id,
            text=f"Choose a plan for {_offer_title(current_offer)}.",
            reply_markup=_build_plans_keyboard(settings, offer=current_offer),
        )
    elif data == "choose_menu":
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
            conn.commit()
            api.answer_callback_query(callback_query_id=_safe(callback_query.get("id")), text=f"Switched to {_offer_title(chosen_offer)}")
            api.send_message(
                chat_id=chat_id,
                text=(
                    f"Current pack: {_offer_title(chosen_offer)}\n"
                    f"{_offer_summary(chosen_offer)}"
                ),
                reply_markup=_main_menu_keyboard(
                    settings,
                    offer=chosen_offer,
                    has_access=(get_active_subscription(conn, user_id=user_id, offer_slug=chosen_offer.slug) is not None),
                ),
            )
            _send_preview_with_pitch(api, conn, settings, offer=chosen_offer, user_id=user_id, chat_id=chat_id)
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
            conn.commit()
            api.answer_callback_query(callback_query_id=_safe(callback_query.get("id")), text="Opening payment")
            _send_plan_invoice(api, settings, offer=offer, chat_id=chat_id, plan=plan)
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
    upsert_bot_user(
        conn,
        BotUser(
            user_id=user_id,
            chat_id=chat_id,
            username=_safe(user.get("username")),
            first_name=_safe(user.get("first_name")),
            last_name=_safe(user.get("last_name")),
        ),
    )
    current_offer = _current_offer(conn, settings, user_id=user_id)
    text = _safe(message.get("text"))
    command = _parse_command(text)

    if command in ("/start", "/help"):
        welcome = (
            f"{settings.bot_name}\n"
            f"Current pack: {_offer_title(current_offer)}\n"
            "I send filtered remote work leads, not generic chat.\n"
            "Use /choose to switch profession packs, /today for the shortlist, or /plans to unlock full access."
        )
        has_access = get_active_subscription(conn, user_id=user_id, offer_slug=current_offer.slug) is not None
        api.send_message(
            chat_id=chat_id,
            text=welcome,
            reply_markup=_main_menu_keyboard(settings, offer=current_offer, has_access=has_access),
        )
        _send_offer_picker(api, settings, chat_id=chat_id, current_offer=current_offer)
        _send_preview_with_pitch(api, conn, settings, offer=current_offer, user_id=user_id, chat_id=chat_id)
    elif command in ("/choose", "/profession", "/professions"):
        _send_offer_picker(api, settings, chat_id=chat_id, current_offer=current_offer)
    elif command == "/today":
        active = get_active_subscription(conn, user_id=user_id, offer_slug=current_offer.slug)
        if active:
            _send_feed(
                api=api,
                conn=conn,
                settings=settings,
                offer=current_offer,
                user_id=user_id,
                chat_id=chat_id,
                limit=_offer_full_limit(current_offer),
                delivery_kind="member_full",
            )
        else:
            _send_preview_with_pitch(api, conn, settings, offer=current_offer, user_id=user_id, chat_id=chat_id)
    elif command in ("/plans", "/buy"):
        api.send_message(
            chat_id=chat_id,
            text=f"Choose a plan for {_offer_title(current_offer)}.",
            reply_markup=_build_plans_keyboard(settings, offer=current_offer),
        )
    elif command == "/status":
        _send_status(api, conn, settings, offer=current_offer, user_id=user_id, chat_id=chat_id)
    elif command == "/terms":
        api.send_message(chat_id=chat_id, text=_build_terms_text(settings))
    elif command in ("/support", "/paysupport"):
        api.send_message(chat_id=chat_id, text=_build_support_text(settings))
    else:
        api.send_message(
            chat_id=chat_id,
            text="Unknown command. Try /choose, /today, /plans, /status, /terms, or /support.",
            reply_markup=_main_menu_keyboard(settings, offer=current_offer, has_access=False),
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
                    _handle_pre_checkout(api, settings, pre_checkout_query=dict(upd.get("pre_checkout_query") or {}))
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
            time.sleep(max(3.0, settings.sleep_sec))
        except Exception as e:
            print(f"[tg-paid-bot] error: {e}")
            time.sleep(max(3.0, settings.sleep_sec))

    try:
        conn.close()
    except Exception:
        pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
