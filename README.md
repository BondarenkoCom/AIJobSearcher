
AIJobSearcher
=============

Practical scanner toolkit for collecting and tracking remote QA/testing leads.

Overview
--------

- Collect job and project leads from public APIs and web sources
- Normalize and store leads in SQLite
- Run deterministic filtering and deduplication
- Run SMTP email outreach workflows
- Inspect results via reports and local UI

Repository layout
-----------------

- `src/` core modules (DB, routing, helpers)
- `scripts/` operational flows (scan/report/SMTP)
- `config/config.yaml` runtime settings
- `templates/` outreach templates
- `ui/` local UI assets
- `Docs/FREELANCE_AUTOMATION_PATH.md` implementation notes

Why Telegram scripts are here
-----------------------------

Telegram scripts in `scripts/telegram_*.py` are used as an additional lead source.

They handle:

- importing job-related channels/chats into a source list
- cleaning/pruning low-signal or spammy sources
- scanning messages and extracting paid gig/job leads into SQLite
- optional folder management for source channels

They do not perform platform apply clicks and do not send Telegram outreach in this public snapshot.

Templates
---------

The `templates/` folder contains SMTP outreach email text samples.

Before real usage, update these templates with your own:

- candidate name and contact details
- portfolio/CV links
- message tone and role-specific value proposition

Do not send stock templates as-is.

Scope of this public snapshot
-----------------------------

- Includes: scanning, filtering, SQLite tracking, SMTP sending.
- Excludes: platform button-click automation (Easy Apply / Apply flows).

Quick start (Windows)
---------------------

```powershell
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
python -m playwright install chromium
Copy-Item .env.example .env
Copy-Item .env.accounts.example .env.accounts
```

Example run:

```powershell
.\.venv\Scripts\python.exe scripts\upwork_scan_jobs.py --write-db --query "qa automation"
```

Security
--------

This repository is a sanitized public snapshot.

- No runtime DB/data exports
- No local sessions
- No private credentials

Keep real credentials only in local `.env` and `.env.accounts` files.

Notes
-----

Use responsibly and follow platform Terms of Service and local laws.
