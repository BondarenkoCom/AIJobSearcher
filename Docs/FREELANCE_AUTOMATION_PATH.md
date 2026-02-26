Freelance Automation Path (API/HTTP-first)
=========================================

Objective
---------

Collect and apply for QA-fit freelance work with:

- maximum coverage,
- minimum bot-risk,
- strict cross-channel dedupe.


Decision rule
-------------

Use this order for every platform:

1. Official API (if stable and accessible).
2. Public JSON/RSS/sitemap/embedded data via HTTP.
3. Browser automation (Playwright) only for authenticated actions (apply, inbox, messages).


Current platforms
-----------------

Freelancermap (`freelancermap.com`)

- Scan path: `freelancermap_scan_projects.py`
  - Source: public sitemap + project page embedded JSON.
  - Browser not required for scanning.
- Apply path: `freelancermap_apply_batch.py`
  - Reason for browser: authenticated apply flow, dynamic UI states, verification gate handling.
- Reply sync: `freelancermap_inbox_sync.py` + `replies_watchdog.py`

Workana (`workana.com`)

- Scan path: `workana_scan_projects.py`
  - Source: jobs HTML with embedded `results-initials` JSON payload.
  - Browser not required for scanning.
- Apply path: `workana_apply_batch.py`
  - Reason for browser: bid flow with session/CSRF/permission checks.
  - Pre-check endpoint used before UI apply to avoid risky blind actions.
- Reply tracking: `replies_watchdog.py` (`wa_apply_submitted -> wa_reply_received`)


Unified run
-----------

Use `scripts/freelance_orchestrator.py` for one-command pipeline:

- scan (Freelancermap + Workana),
- apply (human pacing),
- watchdog status rollup.

Safe default:

- no real submit (dry apply behavior in platform scripts),
- strict QA filtering,
- dedupe skip on any prior contact event.

Enable real submit only with explicit `--submit`.


Operational safety
------------------

- Keep Playwright locale/header in English (`en-US`).
- Keep delays and long breaks enabled for apply flows.
- On blockers/captcha/gates:
  - record `*_needs_manual`,
  - dump debug artifacts,
  - do not brute-force retries.
