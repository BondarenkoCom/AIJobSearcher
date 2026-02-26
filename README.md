
AIJobSearcher
=============

Practical toolkit for collecting and tracking remote QA/testing leads.

Overview
--------

- Collect job and project leads from public APIs and web sources
- Normalize and store leads in SQLite
- Apply deterministic filtering and deduplication
- Run optional outreach/apply workflows with rate limits
- Inspect results via reports and local UI

Repository layout
-----------------

- `src/` core modules (DB, routing, helpers)
- `scripts/` operational flows (scan/apply/report/orchestrators)
- `config/config.yaml` runtime settings
- `templates/` outreach templates
- `ui/` local UI assets
- `Docs/FREELANCE_AUTOMATION_PATH.md` implementation notes

Templates
---------

The `templates/` folder contains outreach email/apply text samples.

Before real usage, update these templates with your own:

- candidate name and contact details
- portfolio/CV links
- message tone and role-specific value proposition

Do not send stock templates as-is.

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
