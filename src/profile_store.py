import re
from datetime import datetime
from typing import Dict, Optional, Tuple


def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def normalize_person_name(name: str) -> str:
    """
    If a name is stored in ALL CAPS (common after copy/paste), normalize it to
    a human-readable Title Case (e.g. "ARTEM BONDARENKO" -> "Candidate Name").
    """
    s = (name or "").strip()
    if not s:
        return ""

    letters = [ch for ch in s if ch.isalpha()]
    if letters and all(ch.isupper() for ch in letters):
        return s.title()
    return s


def normalize_question(text: str) -> str:
    """
    Normalize question text for stable lookup:
    - lowercase
    - remove punctuation
    - collapse whitespace
    """
    t = (text or "").strip().lower()
    t = re.sub(r"[^\w\s]+", " ", t, flags=re.UNICODE)
    t = re.sub(r"\s+", " ", t, flags=re.UNICODE).strip()
    return t


def load_profile(conn) -> Dict[str, str]:
    rows = conn.execute("SELECT key, value FROM profile_kv").fetchall()
    out: Dict[str, str] = {}
    for r in rows:
        k = str(r["key"] or "").strip()
        v = str(r["value"] or "")
        if k:
            out[k] = v
    return out


def get_profile_value(profile: Dict[str, str], key: str, default: str = "") -> str:
    v = (profile or {}).get(key)
    return (v if v is not None else default) or default


def upsert_profile_kv(conn, *, key: str, value: str) -> None:
    k = (key or "").strip()
    if not k:
        return
    conn.execute(
        """
        INSERT INTO profile_kv(key, value, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(key) DO UPDATE SET
          value = excluded.value,
          updated_at = excluded.updated_at
        """,
        (k, (value or "").strip(), now_iso()),
    )


def upsert_document(conn, *, doc_id: str, doc_type: str, content: str) -> None:
    did = (doc_id or "").strip()
    if not did:
        return
    conn.execute(
        """
        INSERT INTO documents(doc_id, doc_type, content, updated_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(doc_id) DO UPDATE SET
          doc_type = excluded.doc_type,
          content = excluded.content,
          updated_at = excluded.updated_at
        """,
        (did, (doc_type or "").strip(), content or "", now_iso()),
    )


def get_answer(conn, q_norm: str) -> Optional[Tuple[str, str]]:
    """
    Returns (answer, status) or None.
    """
    qn = normalize_question(q_norm)
    if not qn:
        return None
    row = conn.execute(
        "SELECT answer, status FROM answer_bank WHERE q_norm = ? LIMIT 1",
        (qn,),
    ).fetchone()
    if not row:
        return None
    return (str(row["answer"] or ""), str(row["status"] or ""))


def insert_answer_if_missing(conn, *, q_raw: str, answer: str, status: str = "confirmed") -> bool:
    """
    Insert without overwriting an existing answer.
    Returns True if inserted.
    """
    qn = normalize_question(q_raw)
    if not qn or not (answer or "").strip():
        return False
    cur = conn.execute(
        """
        INSERT OR IGNORE INTO answer_bank(q_norm, q_raw, answer, status, updated_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (qn, (q_raw or "").strip(), (answer or "").strip(), (status or "confirmed").strip(), now_iso()),
    )
    return bool(getattr(cur, "rowcount", 0) == 1)


def upsert_answer(conn, *, q_raw: str, answer: str, status: str = "confirmed") -> None:
    """
    Upsert and overwrite (used when the user confirms an answer).
    """
    qn = normalize_question(q_raw)
    if not qn or not (answer or "").strip():
        return
    conn.execute(
        """
        INSERT INTO answer_bank(q_norm, q_raw, answer, status, updated_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(q_norm) DO UPDATE SET
          q_raw = excluded.q_raw,
          answer = excluded.answer,
          status = excluded.status,
          updated_at = excluded.updated_at
        """,
        (qn, (q_raw or "").strip(), (answer or "").strip(), (status or "confirmed").strip(), now_iso()),
    )

