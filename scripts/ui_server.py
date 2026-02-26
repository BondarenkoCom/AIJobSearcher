import argparse
import html
import json
import os
import re
import sqlite3
import sys
import threading
import webbrowser
from datetime import datetime
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlencode, urlparse

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.config import cfg_get, load_config, resolve_path  # noqa: E402


def _h(s: Any) -> str:
    return html.escape("" if s is None else str(s), quote=True)


def _short_mid(s: Any, *, max_len: int = 72, tail: int = 18) -> str:
    s = "" if s is None else str(s)
    if len(s) <= max_len:
        return s
    head = max_len - tail - 3
    if head < 8:
        return s[: max_len - 3] + "..."
    return s[:head] + "..." + s[-tail:]


_LI_JOB_RE = re.compile(r"/jobs/view/(\\d+)", re.IGNORECASE)


def _contact_label(platform: str, lead_type: str, contact: Any) -> str:
    c = "" if contact is None else str(contact)
    if platform == "linkedin" and lead_type == "job":
        m = _LI_JOB_RE.search(c)
        if m:
            return f"job:{m.group(1)}"
    if c.startswith("http"):
        return _short_mid(c, max_len=46, tail=12)
    return _short_mid(c, max_len=52, tail=14)


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _int(v: Optional[str], default: int) -> int:
    try:
        return int(v) if v is not None else default
    except Exception:
        return default


def _read_sql(conn: sqlite3.Connection, sql: str, params: Tuple[Any, ...] = ()) -> List[sqlite3.Row]:
    cur = conn.execute(sql, params)
    return cur.fetchall()


def _one(conn: sqlite3.Connection, sql: str, params: Tuple[Any, ...] = ()) -> Optional[sqlite3.Row]:
    cur = conn.execute(sql, params)
    return cur.fetchone()


def _db_connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def _page(title: str, *, active: str, content: str, db_label: str) -> str:
    nav_items = [
        ("dashboard", "/", "Dashboard"),
        ("leads", "/leads", "Leads"),
        ("events", "/events", "Events"),
        ("blocklist", "/blocklist", "Blocklist"),
    ]

    nav_html = []
    for key, href, label in nav_items:
        cls = "nav__link nav__link--active" if key == active else "nav__link"
        nav_html.append(f'<a class="{cls}" href="{_h(href)}">{_h(label)}</a>')

    return f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>{_h(title)}</title>
    <link rel="stylesheet" href="/static/style.css" />
  </head>
  <body class="crt">
    <div class="crt__scan"></div>
    <div class="crt__noise"></div>

    <header class="topbar">
      <div class="brand">
        <div class="brand__logo">JOB OPS CONSOLE</div>
        <div class="brand__sub">1995-grade activity telemetry</div>
      </div>
      <div class="topbar__meta">
        <div class="meta__row"><span class="meta__k">DB</span> <span class="meta__v">{_h(db_label)}</span></div>
        <div class="meta__row"><span class="meta__k">TIME</span> <span class="meta__v">{_h(_now())}</span></div>
      </div>
    </header>

    <div class="layout">
      <nav class="nav">
        <div class="nav__title">NAV</div>
        {''.join(nav_html)}
        <div class="nav__hr"></div>
        <div class="nav__hint">
          <div class="hint__k">MODE</div>
          <div class="hint__v">VIEW + MANUAL MARKS</div>
          <div class="hint__k">NOTE</div>
          <div class="hint__v">Only outreach status can be marked manually.</div>
        </div>
      </nav>

      <main class="main">
        <div class="panel">
          <div class="panel__bar">
            <div class="panel__title">{_h(title)}</div>
            <div class="panel__chip">{_h(active.upper())}</div>
          </div>
          <div class="panel__body">
            {content}
          </div>
        </div>
        <footer class="footer">
          <span class="footer__dot"></span>
          <span class="footer__text">Activity database viewer. Localhost only.</span>
        </footer>
      </main>
    </div>

    <script>
      // Click-to-copy for anything with data-copy.
      document.addEventListener('click', async (e) => {{
        const el = e.target.closest('[data-copy]');
        if (!el) return;
        const txt = el.getAttribute('data-copy') || '';
        try {{
          await navigator.clipboard.writeText(txt);
          el.classList.add('copied');
          setTimeout(() => el.classList.remove('copied'), 650);
        }} catch (_) {{
          // ignore
        }}
      }});
    </script>
  </body>
</html>
"""


def _table(headers: List[str], rows: List[List[str]]) -> str:
    return _table_cls(headers, rows, cls="table")


def _table_cls(headers: List[str], rows: List[List[str]], *, cls: str) -> str:
    th = "".join(f"<th>{_h(h)}</th>" for h in headers)
    trs = []
    for r in rows:
        tds = "".join(f"<td>{c}</td>" for c in r)
        trs.append(f"<tr>{tds}</tr>")
    return f"""
<div class="tablewrap">
  <table class="{_h(cls)}">
    <thead><tr>{th}</tr></thead>
    <tbody>
      {''.join(trs)}
    </tbody>
  </table>
</div>
"""


def _pill(text: str, tone: str = "cyan") -> str:
    cls = f"pill pill--{tone}"
    return f'<span class="{cls}">{_h(text)}</span>'


def _link(href: str, label: str, cls: str = "link") -> str:
    href_s = "" if href is None else str(href)
    attrs = ""
    if href_s.startswith("http://") or href_s.startswith("https://"):
        attrs = ' target="_blank" rel="noopener noreferrer"'
    return f'<a class="{cls}" href="{_h(href_s)}"{attrs}>{_h(label)}</a>'


class UIHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args: Any, directory: str, db_path: Path, db_label: str, **kwargs: Any) -> None:
        self._db_path = db_path
        self._db_label = db_label
        super().__init__(*args, directory=directory, **kwargs)

    def _send_html(self, html_text: str, status: int = 200) -> None:
        data = html_text.encode("utf-8", errors="replace")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _send_json(self, obj: Any, status: int = 200) -> None:
        data = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _redirect(self, location: str, status: int = 303) -> None:
        loc = location if location and location.startswith("/") else "/"
        self.send_response(status)
        self.send_header("Location", loc)
        self.end_headers()

    def _read_form(self) -> Dict[str, List[str]]:
        try:
            length = int(self.headers.get("Content-Length", "0") or "0")
        except Exception:
            length = 0
        if length <= 0:
            return {}
        raw = self.rfile.read(length).decode("utf-8", errors="replace")
        return parse_qs(raw, keep_blank_values=True)

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"
        qs = parse_qs(parsed.query)

        if path == "/api/health":
            ok = self._db_path.exists()
            self._send_json({"ok": ok, "db": str(self._db_path)})
            return

        if path in ("/", "/index.html"):
            self._handle_dashboard()
            return
        if path == "/leads":
            self._handle_leads(qs)
            return
        if path.startswith("/leads/"):
            lead_id = path.split("/", 2)[2] if path.count("/") >= 2 else ""
            self._handle_lead_detail(lead_id)
            return
        if path == "/events":
            self._handle_events(qs)
            return
        if path == "/blocklist":
            self._handle_blocklist()
            return

        return super().do_GET()

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        parts = [p for p in parsed.path.split("/") if p]
        form = self._read_form()

        if len(parts) == 3 and parts[0] == "leads" and parts[2] == "mark-contacted":
            self._handle_mark_contacted(parts[1], form)
            return

        self._send_html(_page("Not Found", active="leads", content="<p class='err'>Unsupported POST route.</p>", db_label=self._db_label), status=404)

    def _open_db(self) -> sqlite3.Connection:
        if not self._db_path.exists():
            raise FileNotFoundError(f"DB not found: {self._db_path}")
        return _db_connect(self._db_path)

    def _handle_mark_contacted(self, lead_id: str, form: Dict[str, List[str]]) -> None:
        lead_id = (lead_id or "").strip()
        next_url = (form.get("next", [""])[0] or "").strip()
        if not next_url.startswith("/"):
            next_url = "/leads"

        if not lead_id:
            self._redirect(next_url)
            return

        try:
            conn = self._open_db()
        except Exception:
            self._redirect(next_url)
            return

        with conn:
            lead = _one(conn, "SELECT lead_id, platform, lead_type, contact, url FROM leads WHERE lead_id = ? LIMIT 1", (lead_id,))
            if not lead:
                self._redirect(next_url)
                return
            if str(lead["platform"]) != "linkedin" or str(lead["lead_type"]) != "post":
                self._redirect(next_url)
                return

            already = _one(
                conn,
                """
                SELECT 1
                FROM events
                WHERE lead_id = ? AND event_type IN ('li_dm_sent', 'li_connect_sent', 'li_comment_posted')
                LIMIT 1
                """,
                (lead_id,),
            )
            if not already:
                details = {
                    "result": "manual_ui_marked",
                    "source": "ui_manual",
                    "profile_url": str(lead["contact"] or ""),
                    "actual_url": str(lead["contact"] or ""),
                    "job_url": str(lead["url"] or ""),
                }
                conn.execute(
                    """
                    INSERT OR IGNORE INTO events (lead_id, event_type, status, occurred_at, details_json)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        lead_id,
                        "li_dm_sent",
                        "ok",
                        _now(),
                        json.dumps(details, ensure_ascii=False, sort_keys=True),
                    ),
                )
                conn.commit()

        self._redirect(next_url)

    def _handle_dashboard(self) -> None:
        try:
            conn = self._open_db()
        except Exception as e:
            self._send_html(_page("Dashboard", active="dashboard", content=f"<p class='err'>{_h(e)}</p>", db_label=self._db_label))
            return

        with conn:
            counts = {
                "leads": int(_one(conn, "SELECT COUNT(*) AS c FROM leads")["c"]),
                "events": int(_one(conn, "SELECT COUNT(*) AS c FROM events")["c"]),
                "blocklist": int(_one(conn, "SELECT COUNT(*) AS c FROM blocklist")["c"]),
            }
            by_type = _read_sql(
                conn, "SELECT event_type, COUNT(*) AS c FROM events GROUP BY event_type ORDER BY c DESC"
            )
            by_day = _read_sql(
                conn,
                """
                SELECT substr(occurred_at, 1, 10) AS day, COUNT(*) AS c
                FROM events
                WHERE event_type = 'email_sent'
                GROUP BY day
                ORDER BY day DESC
                LIMIT 14
                """,
            )
            last_sent = _read_sql(
                conn,
                """
                SELECT e.occurred_at, l.contact, l.company, l.job_title, l.source, l.url
                FROM events e
                JOIN leads l ON l.lead_id = e.lead_id
                WHERE e.event_type = 'email_sent'
                ORDER BY e.occurred_at DESC
                LIMIT 20
                """,
            )

        cards = f"""
<div class="cards">
  <div class="card">
    <div class="card__k">LEADS</div>
    <div class="card__v">{counts['leads']}</div>
    <div class="card__s">unique records</div>
  </div>
  <div class="card">
    <div class="card__k">EVENTS</div>
    <div class="card__v">{counts['events']}</div>
    <div class="card__s">history trail</div>
  </div>
  <div class="card">
    <div class="card__k">BLOCKLIST</div>
    <div class="card__v">{counts['blocklist']}</div>
    <div class="card__s">do-not-contact</div>
  </div>
</div>
"""

        types_rows = []
        max_type = 1
        try:
            max_type = max(int(r["c"]) for r in by_type) if by_type else 1
        except Exception:
            max_type = 1
        for r in by_type:
            t = _pill(str(r["event_type"]), "blue")
            c = _pill(str(r["c"]), "cyan")
            pct = 0.0
            try:
                pct = (int(r["c"]) / max_type) * 100.0 if max_type else 0.0
            except Exception:
                pct = 0.0
            bar = f'<div class="bar"><div class="bar__fill" style="width:{pct:.1f}%"></div></div>'
            types_rows.append([t, c, bar])

        days_rows = []
        max_day = 1
        try:
            max_day = max(int(r["c"]) for r in by_day) if by_day else 1
        except Exception:
            max_day = 1
        for r in by_day:
            pct = 0.0
            try:
                pct = (int(r["c"]) / max_day) * 100.0 if max_day else 0.0
            except Exception:
                pct = 0.0
            bar = f'<div class="bar"><div class="bar__fill bar__fill--amber" style="width:{pct:.1f}%"></div></div>'
            days_rows.append([_pill(str(r["day"]), "amber"), _pill(str(r["c"]), "cyan"), bar])

        sent_rows = []
        for r in last_sent:
            contact = f'<span class="copy" data-copy="{_h(r["contact"])}">{_h(r["contact"])}</span>'
            company = _h(r["company"])
            title = _h(r["job_title"])
            ts = _h(r["occurred_at"])
            sent_rows.append([ts, contact, company, title])

        content = (
            cards
            + "<div class='grid'>"
            + "<section class='subpanel'>"
            + "<div class='subpanel__title'>EVENT TYPES</div>"
            + _table(["type", "count", "signal"], types_rows)
            + "</section>"
            + "<section class='subpanel'>"
            + "<div class='subpanel__title'>EMAIL SENT / DAY</div>"
            + _table(["day", "count", "volume"], days_rows)
            + "</section>"
            + "</div>"
            + "<section class='subpanel'>"
            + "<div class='subpanel__title'>LAST 20 EMAILS SENT</div>"
            + _table(["time", "to", "company", "title"], sent_rows)
            + "</section>"
        )

        self._send_html(_page("Dashboard", active="dashboard", content=content, db_label=self._db_label))

    def _handle_leads(self, qs: Dict[str, List[str]]) -> None:
        q = (qs.get("q", [""])[0] or "").strip()
        platform = (qs.get("platform", [""])[0] or "").strip()
        lead_type = (qs.get("type", [""])[0] or "").strip()
        state = (qs.get("state", [""])[0] or "").strip()
        applied = (qs.get("applied", [""])[0] or "").strip()
        per = max(25, min(200, _int(qs.get("per", [None])[0], 25)))
        page = max(0, _int(qs.get("p", [None])[0], 0))
        off = page * per

        try:
            conn = self._open_db()
        except Exception as e:
            self._send_html(_page("Leads", active="leads", content=f"<p class='err'>{_h(e)}</p>", db_label=self._db_label))
            return

        where = ["1=1"]
        params: List[Any] = []
        if platform:
            where.append("l.platform = ?")
            params.append(platform)
        if lead_type:
            where.append("l.lead_type = ?")
            params.append(lead_type)
        if q:
            where.append(
                "(l.contact LIKE ? OR l.company LIKE ? OR l.job_title LIKE ? OR l.source LIKE ? OR l.url LIKE ?)"
            )
            like = f"%{q}%"
            params.extend([like, like, like, like, like])
        if state == "collected":
            where.append("EXISTS(SELECT 1 FROM events e WHERE e.lead_id = l.lead_id AND e.event_type = 'collected')")
        elif state == "uncollected":
            where.append("NOT EXISTS(SELECT 1 FROM events e WHERE e.lead_id = l.lead_id AND e.event_type = 'collected')")
        if applied == "yes":
            where.append("EXISTS(SELECT 1 FROM events e WHERE e.lead_id = l.lead_id AND e.event_type = 'li_apply_submitted')")
        elif applied == "no":
            where.append("NOT EXISTS(SELECT 1 FROM events e WHERE e.lead_id = l.lead_id AND e.event_type = 'li_apply_submitted')")

        where_sql = " AND ".join(where)
        with conn:
            total = int(
                _one(conn, f"SELECT COUNT(*) AS c FROM leads l WHERE {where_sql}", tuple(params))["c"]
            )

            rows = _read_sql(
                conn,
                f"""
                SELECT
                  l.lead_id, l.platform, l.lead_type, l.contact, l.company, l.job_title, l.location, l.source, l.created_at,
                  EXISTS(SELECT 1 FROM blocklist b WHERE b.contact = l.contact) AS blocked,
                  EXISTS(SELECT 1 FROM events ec WHERE ec.lead_id = l.lead_id AND ec.event_type = 'collected') AS collected,
                  EXISTS(SELECT 1 FROM events ea WHERE ea.lead_id = l.lead_id AND ea.event_type = 'li_apply_submitted') AS applied,
                  EXISTS(SELECT 1 FROM events eo WHERE eo.lead_id = l.lead_id AND eo.event_type IN ('li_dm_sent', 'li_connect_sent', 'li_comment_posted')) AS contacted,
                  (SELECT MAX(occurred_at) FROM events e WHERE e.lead_id = l.lead_id AND e.event_type = 'email_sent') AS last_email_sent
                FROM leads l
                WHERE {where_sql}
                ORDER BY l.created_at DESC
                LIMIT ? OFFSET ?
                """,
                tuple(params + [per, off]),
            )

            platforms = [r["platform"] for r in _read_sql(conn, "SELECT DISTINCT platform FROM leads ORDER BY platform")]
            types = [r["lead_type"] for r in _read_sql(conn, "SELECT DISTINCT lead_type FROM leads ORDER BY lead_type")]

        def qp(**updates: str) -> str:
            base = {"q": q, "platform": platform, "type": lead_type, "state": state, "applied": applied, "per": str(per), "p": str(page)}
            for k, v in updates.items():
                base[k] = v
            return urlencode({k: v for k, v in base.items() if v})

        last_page = max(0, (total - 1) // per) if total else 0
        first_href = f"/leads?{qp(p='0')}"
        prev_href = f"/leads?{qp(p=str(max(0, page - 1)))}"
        next_href = f"/leads?{qp(p=str(min(last_page, page + 1)))}"
        last_href = f"/leads?{qp(p=str(last_page))}"

        filters = f"""
<form class="filters" method="get" action="/leads">
  <div class="filters__row">
    <label class="field">
      <span class="field__k">SEARCH</span>
      <input class="field__in" type="text" name="q" value="{_h(q)}" placeholder="company / email / title / source" />
    </label>
    <label class="field">
      <span class="field__k">PLATFORM</span>
      <select class="field__in" name="platform">
        <option value="">(all)</option>
        {''.join(f'<option value="{_h(p)}"' + (' selected' if p==platform else '') + f'>{_h(p)}</option>' for p in platforms)}
      </select>
    </label>
    <label class="field">
      <span class="field__k">TYPE</span>
      <select class="field__in" name="type">
        <option value="">(all)</option>
        {''.join(f'<option value="{_h(t)}"' + (' selected' if t==lead_type else '') + f'>{_h(t)}</option>' for t in types)}
      </select>
    </label>
    <label class="field">
      <span class="field__k">STATE</span>
      <select class="field__in" name="state">
        <option value="">(all)</option>
        <option value="collected"{' selected' if state=='collected' else ''}>collected</option>
        <option value="uncollected"{' selected' if state=='uncollected' else ''}>uncollected</option>
      </select>
    </label>
    <label class="field">
      <span class="field__k">APPLIED</span>
      <select class="field__in" name="applied">
        <option value="">(all)</option>
        <option value="yes"{' selected' if applied=='yes' else ''}>applied</option>
        <option value="no"{' selected' if applied=='no' else ''}>not applied</option>
      </select>
    </label>
    <label class="field field--mini">
      <span class="field__k">PER</span>
      <input class="field__in" type="number" name="per" min="25" max="200" value="{_h(per)}" />
    </label>
    <button class="btn" type="submit">RUN</button>
  </div>
</form>
"""

        hud = f"""
<div class="hud">
  <div class="hud__left">
    {_pill(f'{total} rows', 'cyan')}
    {_pill(f'page {page+1}/{last_page+1}', 'blue')}
  </div>
  <div class="hud__right">
    <a class="btn btn--ghost" href="{_h(first_href)}">First</a>
    <a class="btn btn--ghost" href="{_h(prev_href)}">Prev</a>
    <a class="btn btn--ghost" href="{_h(next_href)}">Next</a>
    <a class="btn btn--ghost" href="{_h(last_href)}">Last</a>
  </div>
</div>
"""

        table_rows: List[List[str]] = []
        current_qs = qp()
        current_href = f"/leads?{current_qs}" if current_qs else "/leads"
        for r in rows:
            lid = r["lead_id"]
            link = _link(f"/leads/{lid}", "open", cls="btn btn--tiny")
            full_contact = r["contact"]
            label = _contact_label(str(r["platform"]), str(r["lead_type"]), full_contact)
            contact = f'<span class="copy" title="{_h(full_contact)}" data-copy="{_h(full_contact)}">{_h(label)}</span>'
            blocked = _pill("BLOCK", "red") if r["blocked"] else ""
            state_pill = _pill("COLLECTED", "blue") if r["collected"] else _pill("CANDIDATE", "cyan")
            applied_pill = _pill("APPLIED", "amber") if r["applied"] else ""
            contacted_pill = _pill("WROTE", "amber") if r["contacted"] else ""
            last_sent = _pill(r["last_email_sent"][:19], "amber") if r["last_email_sent"] else ""
            company_raw = r["company"] or ""
            title_raw = r["job_title"] or ""
            loc_raw = r["location"] or ""
            company = _h(company_raw)
            title = _h(title_raw)
            loc = _h(loc_raw)
            mark_btn = ""
            if str(r["platform"]) == "linkedin" and str(r["lead_type"]) == "post" and not r["contacted"]:
                mark_btn = (
                    f'<form class="inlineform" method="post" action="/leads/{_h(lid)}/mark-contacted">'
                    f'<input type="hidden" name="next" value="{_h(current_href)}" />'
                    '<button class="btn btn--tiny" type="submit">mark wrote</button>'
                    "</form>"
                )
            table_rows.append(
                [
                    link,
                    mark_btn,
                    _pill(r["platform"], "blue"),
                    _pill(r["lead_type"], "cyan"),
                    contact
                    + (" " + blocked if blocked else "")
                    + (" " + applied_pill if applied_pill else "")
                    + (" " + contacted_pill if contacted_pill else "")
                    + " "
                    + state_pill,
                    (f'<span title="{company}">{company}</span>' if company else ""),
                    (f'<span title="{title}">{title}</span>' if title else ""),
                    (f'<span title="{loc}">{loc}</span>' if loc else ""),
                    last_sent,
                ]
            )

        content = (
            filters
            + hud
            + _table_cls(
                ["", "action", "platform", "type", "contact", "company", "title", "location", "last email_sent"],
                table_rows,
                cls="table table--leads",
            )
        )
        self._send_html(_page("Leads", active="leads", content=content, db_label=self._db_label))

    def _handle_lead_detail(self, lead_id: str) -> None:
        lead_id = (lead_id or "").strip()
        if not lead_id:
            self._send_html(_page("Lead", active="leads", content="<p class='err'>Missing lead_id.</p>", db_label=self._db_label))
            return

        try:
            conn = self._open_db()
        except Exception as e:
            self._send_html(_page("Lead", active="leads", content=f"<p class='err'>{_h(e)}</p>", db_label=self._db_label))
            return

        with conn:
            lead = _one(conn, "SELECT * FROM leads WHERE lead_id = ? LIMIT 1", (lead_id,))
            if not lead:
                self._send_html(
                    _page("Lead", active="leads", content="<p class='err'>Not found.</p>", db_label=self._db_label),
                    status=404,
                )
                return
            blocked = bool(_one(conn, "SELECT 1 FROM blocklist WHERE contact = ? LIMIT 1", (lead["contact"],)))
            contacted = bool(
                _one(
                    conn,
                    """
                    SELECT 1
                    FROM events
                    WHERE lead_id = ? AND event_type IN ('li_dm_sent', 'li_connect_sent', 'li_comment_posted')
                    LIMIT 1
                    """,
                    (lead_id,),
                )
            )
            events = _read_sql(
                conn,
                "SELECT event_type, status, occurred_at, details_json FROM events WHERE lead_id = ? ORDER BY occurred_at DESC LIMIT 500",
                (lead_id,),
            )

        lead_rows = [
            ["lead_id", f"<code class='code'>{_h(lead['lead_id'])}</code>"],
            ["platform", _pill(lead["platform"], "blue")],
            ["type", _pill(lead["lead_type"], "cyan")],
            ["contact", f'<span class="copy" data-copy="{_h(lead["contact"])}">{_h(lead["contact"])}</span>' + (" " + _pill("BLOCK", "red") if blocked else "")],
            ["company", _h(lead["company"])],
            ["job_title", _h(lead["job_title"])],
            ["location", _h(lead["location"])],
            ["source", _h(lead["source"])],
            ["url", _link(lead["url"], lead["url"][:60] + ("..." if len(lead["url"]) > 60 else ""), cls="link") if lead["url"] else ""],
            ["created_at", _h(lead["created_at"])],
        ]

        ev_rows = []
        for e in events:
            details = e["details_json"] or ""
            if details and len(details) > 140:
                details = details[:140] + "..."
            ev_rows.append(
                [
                    _pill(e["event_type"], "blue"),
                    _pill(e["status"], "cyan" if e["status"] == "ok" else "amber"),
                    _h(e["occurred_at"]),
                    f"<code class='code'>{_h(details)}</code>",
                ]
            )

        raw_json = lead["raw_json"] or ""
        raw_block = ""
        if raw_json:
            raw_block = f"""
<details class="details">
  <summary>raw_json</summary>
  <pre class="pre">{_h(raw_json)}</pre>
</details>
"""

        content = (
            "<div class='hud'><div class='hud__left'>"
            + _link("/leads", "Back to leads", cls="btn btn--ghost")
            + "</div></div>"
            + (
                "<section class='subpanel'><div class='subpanel__title'>ACTIONS</div>"
                + (
                    _pill("WROTE TO LEAD", "amber")
                    if contacted
                    else (
                        f'<div class="actions">'
                        f'<form class="inlineform" method="post" action="/leads/{_h(lead_id)}/mark-contacted">'
                        f'<input type="hidden" name="next" value="/leads/{_h(lead_id)}" />'
                        '<button class="btn" type="submit">Mark as wrote to lead</button>'
                        "</form>"
                        "</div>"
                    )
                )
                + "</section>"
                if str(lead["platform"]) == "linkedin" and str(lead["lead_type"]) == "post"
                else ""
            )
            + "<section class='subpanel'><div class='subpanel__title'>LEAD</div>"
            + _table(["field", "value"], lead_rows)
            + raw_block
            + "</section>"
            + "<section class='subpanel'><div class='subpanel__title'>EVENTS (last 500)</div>"
            + _table(["type", "status", "time", "details"], ev_rows)
            + "</section>"
        )

        self._send_html(_page("Lead Detail", active="leads", content=content, db_label=self._db_label))

    def _handle_events(self, qs: Dict[str, List[str]]) -> None:
        q = (qs.get("q", [""])[0] or "").strip()
        etype = (qs.get("type", [""])[0] or "").strip()
        per = max(25, min(500, _int(qs.get("per", [None])[0], 200)))
        page = max(0, _int(qs.get("p", [None])[0], 0))
        off = page * per

        try:
            conn = self._open_db()
        except Exception as e:
            self._send_html(_page("Events", active="events", content=f"<p class='err'>{_h(e)}</p>", db_label=self._db_label))
            return

        where = ["1=1"]
        params: List[Any] = []
        if etype:
            where.append("e.event_type = ?")
            params.append(etype)
        if q:
            where.append("(l.contact LIKE ? OR l.company LIKE ? OR l.job_title LIKE ? OR l.source LIKE ?)")
            like = f"%{q}%"
            params.extend([like, like, like, like])

        where_sql = " AND ".join(where)

        with conn:
            total = int(
                _one(
                    conn,
                    f"SELECT COUNT(*) AS c FROM events e JOIN leads l ON l.lead_id = e.lead_id WHERE {where_sql}",
                    tuple(params),
                )["c"]
            )
            types = [r["event_type"] for r in _read_sql(conn, "SELECT DISTINCT event_type FROM events ORDER BY event_type")]

            rows = _read_sql(
                conn,
                f"""
                SELECT
                  e.occurred_at, e.event_type, e.status,
                  l.platform, l.contact, l.company, l.job_title, l.source
                FROM events e
                JOIN leads l ON l.lead_id = e.lead_id
                WHERE {where_sql}
                ORDER BY e.occurred_at DESC
                LIMIT ? OFFSET ?
                """,
                tuple(params + [per, off]),
            )

        last_page = max(0, (total - 1) // per) if total else 0
        prev_href = f"/events?{urlencode({'q': q, 'type': etype, 'per': per, 'p': max(0, page-1)})}"
        next_href = f"/events?{urlencode({'q': q, 'type': etype, 'per': per, 'p': min(last_page, page+1)})}"

        filters = f"""
<form class="filters" method="get" action="/events">
  <div class="filters__row">
    <label class="field">
      <span class="field__k">SEARCH</span>
      <input class="field__in" type="text" name="q" value="{_h(q)}" placeholder="contact / company / title / source" />
    </label>
    <label class="field">
      <span class="field__k">TYPE</span>
      <select class="field__in" name="type">
        <option value="">(all)</option>
        {''.join(f'<option value="{_h(t)}"' + (' selected' if t==etype else '') + f'>{_h(t)}</option>' for t in types)}
      </select>
    </label>
    <label class="field field--mini">
      <span class="field__k">PER</span>
      <input class="field__in" type="number" name="per" min="25" max="500" value="{_h(per)}" />
    </label>
    <button class="btn" type="submit">RUN</button>
  </div>
</form>
"""

        hud = f"""
<div class="hud">
  <div class="hud__left">
    {_pill(f'{total} rows', 'cyan')}
    {_pill(f'page {page+1}/{last_page+1}', 'blue')}
  </div>
  <div class="hud__right">
    <a class="btn btn--ghost" href="{_h(prev_href)}">Prev</a>
    <a class="btn btn--ghost" href="{_h(next_href)}">Next</a>
  </div>
</div>
"""

        table_rows: List[List[str]] = []
        for r in rows:
            contact = f'<span class="copy" data-copy="{_h(r["contact"])}">{_h(r["contact"])}</span>'
            table_rows.append(
                [
                    _h(r["occurred_at"]),
                    _pill(r["event_type"], "blue"),
                    _pill(r["status"], "cyan" if r["status"] == "ok" else "amber"),
                    _pill(r["platform"], "cyan"),
                    contact,
                    _h(r["company"]),
                    _h(r["job_title"]),
                    _h(r["source"]),
                ]
            )

        content = filters + hud + _table(["time", "type", "status", "platform", "contact", "company", "title", "source"], table_rows)
        self._send_html(_page("Events", active="events", content=content, db_label=self._db_label))

    def _handle_blocklist(self) -> None:
        try:
            conn = self._open_db()
        except Exception as e:
            self._send_html(_page("Blocklist", active="blocklist", content=f"<p class='err'>{_h(e)}</p>", db_label=self._db_label))
            return

        with conn:
            rows = _read_sql(conn, "SELECT contact, reason, created_at FROM blocklist ORDER BY created_at DESC")

        table_rows = []
        for r in rows:
            contact = f'<span class="copy" data-copy="{_h(r["contact"])}">{_h(r["contact"])}</span>'
            table_rows.append([contact, _h(r["reason"]), _h(r["created_at"])])

        content = _table(["contact", "reason", "created_at"], table_rows)
        self._send_html(_page("Blocklist", active="blocklist", content=content, db_label=self._db_label))


def main() -> int:
    ap = argparse.ArgumentParser(description="Activity UI for activity.sqlite (retro 1995 style)")
    ap.add_argument("--config", default="config/config.yaml", help="Config YAML (default: config/config.yaml)")
    ap.add_argument("--db", default="", help="Override DB path (default: activity.db_path from config)")
    ap.add_argument("--host", default="127.0.0.1", help="Bind host (default: 127.0.0.1)")
    ap.add_argument("--port", type=int, default=8787, help="Bind port (default: 8787)")
    ap.add_argument("--no-open", action="store_true", help="Do not open browser automatically")
    args = ap.parse_args()

    cfg_path = resolve_path(ROOT, args.config)
    cfg = load_config(str(cfg_path)) if cfg_path.exists() else {}

    db_path = resolve_path(ROOT, str(cfg_get(cfg, "activity.db_path", "data/out/activity.sqlite")))
    if args.db.strip():
        db_path = resolve_path(ROOT, args.db.strip())

    ui_dir = (ROOT / "ui").resolve()
    directory = str(ui_dir)

    db_label = str(db_path)
    if len(db_label) > 64:
        db_label = "..." + db_label[-63:]

    handler = partial(UIHandler, directory=directory, db_path=db_path, db_label=db_label)
    httpd = ThreadingHTTPServer((args.host, args.port), handler)

    url = f"http://{args.host}:{args.port}/"
    print(f"[ui] db={db_path}")
    print(f"[ui] serving {url}")

    if not args.no_open:
        def _open() -> None:
            try:
                webbrowser.open(url, new=2)
            except Exception:
                pass

        threading.Thread(target=_open, daemon=True).start()

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("[ui] stopping...")
    finally:
        httpd.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
