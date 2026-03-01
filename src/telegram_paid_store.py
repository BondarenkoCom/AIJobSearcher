from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, Optional


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _safe(value: Any) -> str:
    return str(value or "").strip()


def _pref_key(user_id: int, name: str) -> str:
    return f"bot_pref:{int(user_id)}:{_safe(name).lower()}"


@dataclass
class BotUser:
    user_id: int
    chat_id: int
    username: str = ""
    first_name: str = ""
    last_name: str = ""


def upsert_bot_user(conn: sqlite3.Connection, user: BotUser) -> None:
    now = _now_iso()
    conn.execute(
        """
        INSERT INTO bot_users (user_id, chat_id, username, first_name, last_name, is_active, created_at, updated_at, last_seen_at)
        VALUES (?, ?, ?, ?, ?, 1, ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
          chat_id = excluded.chat_id,
          username = excluded.username,
          first_name = excluded.first_name,
          last_name = excluded.last_name,
          is_active = 1,
          updated_at = excluded.updated_at,
          last_seen_at = excluded.last_seen_at
        """,
        (
            int(user.user_id),
            int(user.chat_id),
            _safe(user.username),
            _safe(user.first_name),
            _safe(user.last_name),
            now,
            now,
            now,
        ),
    )


def set_user_selected_offer(conn: sqlite3.Connection, *, user_id: int, offer_slug: str) -> None:
    now = _now_iso()
    conn.execute(
        """
        INSERT INTO profile_kv (key, value, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(key) DO UPDATE SET
          value = excluded.value,
          updated_at = excluded.updated_at
        """,
        (_pref_key(user_id, "selected_offer"), _safe(offer_slug), now),
    )


def get_user_selected_offer(conn: sqlite3.Connection, *, user_id: int) -> str:
    row = conn.execute(
        """
        SELECT value
        FROM profile_kv
        WHERE key = ?
        LIMIT 1
        """,
        (_pref_key(user_id, "selected_offer"),),
    ).fetchone()
    return _safe(row["value"]) if row is not None else ""


def get_active_subscription(conn: sqlite3.Connection, *, user_id: int, offer_slug: str) -> Optional[Dict[str, Any]]:
    now = _now_iso()
    row = conn.execute(
        """
        SELECT *
        FROM bot_subscriptions
        WHERE user_id = ?
          AND offer_slug = ?
          AND status = 'active'
          AND datetime(ends_at) > datetime(?)
        ORDER BY datetime(ends_at) DESC, subscription_id DESC
        LIMIT 1
        """,
        (int(user_id), _safe(offer_slug), now),
    ).fetchone()
    return dict(row) if row is not None else None


def add_payment_and_grant_access(
    conn: sqlite3.Connection,
    *,
    user_id: int,
    offer_slug: str,
    plan_code: str,
    days: int,
    charge_id: str,
    invoice_payload: str,
    currency: str,
    total_amount: int,
    is_recurring: bool,
    raw_payment: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    existing_payment = conn.execute(
        """
        SELECT telegram_payment_charge_id
        FROM bot_payments
        WHERE telegram_payment_charge_id = ?
        LIMIT 1
        """,
        (_safe(charge_id),),
    ).fetchone()
    if existing_payment is not None:
        active = get_active_subscription(conn, user_id=int(user_id), offer_slug=offer_slug)
        return {
            "starts_at": _safe((active or {}).get("starts_at")),
            "ends_at": _safe((active or {}).get("ends_at")),
        }

    now = datetime.now()
    now_iso = now.isoformat(timespec="seconds")
    existing = get_active_subscription(conn, user_id=int(user_id), offer_slug=offer_slug)
    if existing:
        try:
            base = datetime.fromisoformat(str(existing.get("ends_at") or ""))
        except Exception:
            base = now
    else:
        base = now
    starts_at = now_iso
    ends_at = (base + timedelta(days=max(1, int(days)))).isoformat(timespec="seconds")

    conn.execute(
        """
        INSERT OR REPLACE INTO bot_payments
        (telegram_payment_charge_id, user_id, offer_slug, plan_code, invoice_payload, currency, total_amount, status, paid_at, is_recurring, raw_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, 'paid', ?, ?, ?)
        """,
        (
            _safe(charge_id),
            int(user_id),
            _safe(offer_slug),
            _safe(plan_code),
            _safe(invoice_payload),
            _safe(currency),
            int(total_amount),
            now_iso,
            1 if bool(is_recurring) else 0,
            json.dumps(raw_payment or {}, ensure_ascii=False, sort_keys=True),
        ),
    )

    conn.execute(
        """
        INSERT INTO bot_subscriptions
        (user_id, offer_slug, plan_code, status, starts_at, ends_at, source, telegram_payment_charge_id, is_recurring, created_at, updated_at, raw_json)
        VALUES (?, ?, ?, 'active', ?, ?, 'telegram_stars', ?, ?, ?, ?, ?)
        """,
        (
            int(user_id),
            _safe(offer_slug),
            _safe(plan_code),
            starts_at,
            ends_at,
            _safe(charge_id),
            1 if bool(is_recurring) else 0,
            now_iso,
            now_iso,
            json.dumps(raw_payment or {}, ensure_ascii=False, sort_keys=True),
        ),
    )
    return {
        "starts_at": starts_at,
        "ends_at": ends_at,
    }


def log_delivery(
    conn: sqlite3.Connection,
    *,
    user_id: int,
    offer_slug: str,
    delivery_kind: str,
    item_count: int,
    message_id: Optional[int] = None,
    details: Optional[Dict[str, Any]] = None,
) -> None:
    conn.execute(
        """
        INSERT INTO bot_delivery_log (user_id, offer_slug, delivery_kind, item_count, message_id, created_at, details_json)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            int(user_id),
            _safe(offer_slug),
            _safe(delivery_kind),
            max(0, int(item_count)),
            None if message_id is None else int(message_id),
            _now_iso(),
            json.dumps(details or {}, ensure_ascii=False, sort_keys=True),
        ),
    )


def get_user_summary(conn: sqlite3.Connection, *, user_id: int, offer_slug: str) -> Dict[str, Any]:
    sub = get_active_subscription(conn, user_id=user_id, offer_slug=offer_slug)
    payments = conn.execute(
        """
        SELECT COUNT(*) AS c
        FROM bot_payments
        WHERE user_id = ? AND offer_slug = ?
        """,
        (int(user_id), _safe(offer_slug)),
    ).fetchone()
    deliveries = conn.execute(
        """
        SELECT COUNT(*) AS c
        FROM bot_delivery_log
        WHERE user_id = ? AND offer_slug = ?
        """,
        (int(user_id), _safe(offer_slug)),
    ).fetchone()
    return {
        "active_subscription": sub,
        "payments_count": int((payments or {"c": 0})["c"]),
        "deliveries_count": int((deliveries or {"c": 0})["c"]),
    }
