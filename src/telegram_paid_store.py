from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional


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


def log_bot_event(
    conn: sqlite3.Connection,
    *,
    user_id: int,
    chat_id: int,
    offer_slug: str,
    event_type: str,
    status: str = "ok",
    details: Optional[Dict[str, Any]] = None,
) -> None:
    conn.execute(
        """
        INSERT INTO bot_event_log (user_id, chat_id, offer_slug, event_type, status, created_at, details_json)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            int(user_id),
            int(chat_id),
            _safe(offer_slug),
            _safe(event_type),
            _safe(status) or "ok",
            _now_iso(),
            json.dumps(details or {}, ensure_ascii=False, sort_keys=True),
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


def _count(conn: sqlite3.Connection, query: str, params: tuple = ()) -> int:
    row = conn.execute(query, params).fetchone()
    if row is None:
        return 0
    try:
        return int(row["c"])
    except Exception:
        return int(row[0])


def get_bot_analytics_summary(conn: sqlite3.Connection) -> Dict[str, Any]:
    now = datetime.now()
    since_1d = (now - timedelta(days=1)).isoformat(timespec="seconds")
    since_7d = (now - timedelta(days=7)).isoformat(timespec="seconds")

    def event_stats(event_type: str, *, status: str = "") -> Dict[str, int]:
        where = ["event_type = ?"]
        params: List[Any] = [event_type]
        if _safe(status):
            where.append("status = ?")
            params.append(_safe(status))
        where_sql = " AND ".join(where)
        return {
            "total": _count(conn, f"SELECT COUNT(*) AS c FROM bot_event_log WHERE {where_sql}", tuple(params)),
            "unique_users": _count(conn, f"SELECT COUNT(DISTINCT user_id) AS c FROM bot_event_log WHERE {where_sql}", tuple(params)),
        }

    starts = event_stats("start")
    plans_opened = event_stats("plans_opened")
    buy_clicked = event_stats("buy_clicked")
    invoices_sent = event_stats("invoice_sent")
    pre_checkout_ok = event_stats("pre_checkout", status="ok")
    pre_checkout_fail = event_stats("pre_checkout", status="fail")

    previews_sent = _count(conn, "SELECT COUNT(*) AS c FROM bot_delivery_log WHERE delivery_kind = 'preview'")
    full_sent = _count(conn, "SELECT COUNT(*) AS c FROM bot_delivery_log WHERE delivery_kind IN ('member_full', 'paid_full')")
    total_payments = _count(conn, "SELECT COUNT(*) AS c FROM bot_payments WHERE status = 'paid'")
    unique_payers = _count(conn, "SELECT COUNT(DISTINCT user_id) AS c FROM bot_payments WHERE status = 'paid'")
    stars_revenue = _count(conn, "SELECT COALESCE(SUM(total_amount), 0) AS c FROM bot_payments WHERE status = 'paid'")
    active_subscriptions = _count(
        conn,
        """
        SELECT COUNT(*) AS c
        FROM bot_subscriptions
        WHERE status = 'active' AND datetime(ends_at) > datetime(?)
        """,
        (now.isoformat(timespec="seconds"),),
    )
    unique_users_by_pack = conn.execute(
        """
        SELECT offer_slug, COUNT(DISTINCT user_id) AS unique_users, COUNT(*) AS visits
        FROM bot_event_log
        WHERE event_type = 'start' AND offer_slug != ''
        GROUP BY offer_slug
        ORDER BY unique_users DESC, visits DESC
        LIMIT 5
        """
    ).fetchall()
    payments_by_pack = conn.execute(
        """
        SELECT offer_slug, COUNT(*) AS payments, COUNT(DISTINCT user_id) AS unique_payers, COALESCE(SUM(total_amount), 0) AS stars
        FROM bot_payments
        WHERE status = 'paid' AND offer_slug != ''
        GROUP BY offer_slug
        ORDER BY stars DESC, payments DESC
        LIMIT 5
        """
    ).fetchall()

    return {
        "users_total": _count(conn, "SELECT COUNT(*) AS c FROM bot_users"),
        "active_users_24h": _count(conn, "SELECT COUNT(DISTINCT user_id) AS c FROM bot_event_log WHERE datetime(created_at) >= datetime(?)", (since_1d,)),
        "active_users_7d": _count(conn, "SELECT COUNT(DISTINCT user_id) AS c FROM bot_event_log WHERE datetime(created_at) >= datetime(?)", (since_7d,)),
        "starts": starts,
        "returning_users": _count(
            conn,
            """
            SELECT COUNT(*) AS c
            FROM (
              SELECT user_id
              FROM bot_event_log
              WHERE event_type = 'start'
              GROUP BY user_id
              HAVING COUNT(*) > 1
            )
            """,
        ),
        "plans_opened": plans_opened,
        "buy_clicked": buy_clicked,
        "invoices_sent": invoices_sent,
        "pre_checkout_ok": pre_checkout_ok,
        "pre_checkout_fail": pre_checkout_fail,
        "payments_total": total_payments,
        "unique_payers": unique_payers,
        "stars_revenue": stars_revenue,
        "active_subscriptions": active_subscriptions,
        "preview_deliveries": previews_sent,
        "full_deliveries": full_sent,
        "top_packs_by_users": [dict(row) for row in unique_users_by_pack],
        "top_packs_by_payments": [dict(row) for row in payments_by_pack],
    }
