Freelance Automation Path (API/HTTP-first)
=========================================

Objective
---------

Collect QA-fit freelance opportunities with:

- maximum coverage,
- minimum bot-risk,
- strict cross-channel dedupe.


Decision rule
-------------

Use this order for every platform:

1. Official API (if stable and accessible).
2. Public JSON/RSS/sitemap/embedded data via HTTP.
3. Browser automation only for read-only scans when HTTP data is insufficient.


Current platforms
-----------------

Freelancermap (`freelancermap.com`)

- Scan path: `freelancermap_scan_projects.py`
  - Source: public sitemap + project page embedded JSON.
  - Browser not required for scanning.

Workana (`workana.com`)

- Scan path: `workana_scan_projects.py`
  - Source: jobs HTML with embedded `results-initials` JSON payload.
  - Browser not required for scanning.


Unified run
-----------

Use scanner scripts + SMTP sender for one-command pipeline:

- scan (Freelancermap + Workana),
- prepare shortlist,
- send SMTP outreach.

Safe default:

- no platform-side auto-apply clicks,
- strict QA filtering,
- dedupe skip on any prior contact event.


Operational safety
------------------

- Keep Playwright locale/header in English (`en-US`).
- Keep delays and long breaks enabled for outbound SMTP sends.
- On blockers/captcha/gates:
  - mark for manual review,
  - dump debug artifacts,
  - do not brute-force retries.
