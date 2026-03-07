AIJobSearcher
=============

![AIJobSearcher cover](assets/aya_operator_cover.png)

Remote work lead engine with a paid Telegram delivery layer.

Overview
--------

This repository is a practical system for:

- collecting remote jobs, gigs, and contract leads from public sources
- normalizing and storing leads in SQLite
- building profession-specific shortlists
- delivering those shortlists through a Telegram bot
- gating premium AI features behind Telegram Stars payments

The current public product layer is branded as `Remote Work Hunter`.

What The Public Snapshot Can Do
-------------------------------

- scan public job boards, forums, and freelance platforms
- store leads and bot analytics in SQLite
- build sellable offer feeds from `config/offers.yaml`
- run a Telegram bot with:
  - profession pack selection
  - stack selection
  - preview vs paid full shortlist
  - source transparency
  - Telegram Stars payments
  - AI apply analysis
  - AI-generated cover notes
- keep uploaded CV text only in temporary bot memory, not in long-term DB storage

Current Profession Packs
------------------------

- `qa_gig_hunter`
- `software_engineering_hunter`
- `data_ai_hunter`
- `cybersecurity_hunter`
- `devops_cloud_hunter`

Current Bot Logic
-----------------

Free users:

- can open the bot
- can choose a profession pack
- can choose a stack
- can get preview leads
- do not see AI match scores
- do not get AI apply analysis or cover generation

Paid or privileged users:

- can get the full shortlist
- can see per-lead AI score in the shortlist
- can run `Apply analysis`
- can generate tailored cover notes

Important:

- AI scoring and AI apply analysis require a temporary CV
- if the user has access but no CV loaded, the bot asks for CV first
- CV text is processed transiently in memory only

What The Public Snapshot Does Not Include
-----------------------------------------

- final platform submit automation
- blind auto-apply across third-party sites
- private sessions and private credentials
- private runtime DB dumps

This public snapshot is intentionally sanitized.

Repository Layout
-----------------

- `src/` core modules, storage, scoring, bot support, helpers
- `scripts/` scanners, reports, offer pipelines, Telegram bot flows
- `config/` runtime config and offer definitions
- `Docs/` product notes, bot notes, scanner matrix, implementation status
- `templates/` SMTP outreach templates
- `ui/` local UI assets

Main Runtime Paths
------------------

- `config/config.yaml` core scanner/runtime settings
- `config/offers.yaml` profession packs, bot text, plans, stack options
- `scripts/run_offer_pipeline.py` run one offer end-to-end
- `scripts/export_offer_feed.py` export a sellable feed
- `scripts/telegram_paid_bot.py` paid Telegram bot
- `scripts/run_bot_stack.py` background loop for bot + offer refresh

Key Scanner Families
--------------------

Public and generic:

- `web_scan_contract_jobs.py`
- `hn_scan_whoishiring.py`
- HTTP/API collectors in `src/collectors/`

Freelance and gig-oriented:

- `workana_scan_projects.py`
- `freelancermap_scan_projects.py`
- `reddit_scan_gigs.py`
- `upwork_scan_jobs.py` (optional)
- `telegram_scan_gigs.py` (optional)

LinkedIn and other logged-in sources:

- `linkedin_scan_jobs.py` (optional)
- `linkedin_scan_posts.py` (optional)

Quick Start (Windows)
---------------------

```powershell
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
python -m playwright install chromium
Copy-Item .env.example .env
Copy-Item .env.accounts.example .env.accounts
```

Run One Offer
-------------

```powershell
.\.venv\Scripts\python.exe scripts\run_offer_pipeline.py --offer qa_gig_hunter
```

Include Optional Scanners
-------------------------

```powershell
.\.venv\Scripts\python.exe scripts\run_offer_pipeline.py --offer qa_gig_hunter --with-optional
```

Run The Telegram Bot Locally
----------------------------

```powershell
.\.venv\Scripts\python.exe scripts\telegram_paid_bot.py --offer qa_gig_hunter
```

Run The Snake Web App Locally
-----------------------------

```powershell
.\.venv\Scripts\python.exe scripts\snake_webapp_server.py --port 8790
```

Set `TELEGRAM_WEBAPP_URL` to the public HTTPS URL that serves `/snake/`.
For local Telegram testing, use a tunnel and point the bot button to that HTTPS URL.

Run The Combined Bot Stack
--------------------------

```powershell
.\.venv\Scripts\python.exe scripts\run_bot_stack.py --default-offer qa_gig_hunter --short-limit 12 --refresh-hours 6
```

Telegram Product Model
----------------------

The current Telegram bot flow is:

1. User opens `Remote Work Hunter`
2. User chooses a profession pack
3. User optionally chooses a stack
4. Free user receives preview leads
5. Paid user receives full shortlist
6. Paid user with CV receives:
   - per-lead AI score in shortlist
   - `Apply analysis`
   - tailored cover note generation

See also:

- `Docs/TELEGRAM_BOT_MVP.md`
- `Docs/AIJobSearcher_Implemented_Status.md`
- `Docs/PROFESSION_SCANNER_MATRIX.txt`

Security And Privacy
--------------------

- Keep real credentials only in local `.env` and `.env.accounts`
- Do not commit runtime DB exports
- CV text sent through `/cv` is temporary and not written to long-term bot storage

Notes
-----

Use responsibly and follow platform Terms of Service and local laws.
