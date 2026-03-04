Public Implemented Status
=========================

This file describes the current public snapshot status of the repository.

Core Engine
-----------

Implemented:

- multi-source lead collection into SQLite
- deterministic filtering and deduplication
- offer-specific export layer
- profession packs and stack filters
- bot analytics and delivery tracking

Current Offer Packs
-------------------

- `qa_gig_hunter`
- `software_engineering_hunter`
- `data_ai_hunter`
- `cybersecurity_hunter`
- `devops_cloud_hunter`

Telegram Product Layer
----------------------

Implemented:

- bot brand: `Remote Work Hunter`
- profession pack chooser
- stack chooser
- preview vs paid shortlist flow
- source transparency screen
- Telegram Stars one-time payments
- admin/free tester bypass lists
- AI apply analysis
- AI-generated cover notes

Current AI Rules
----------------

- free users do not receive AI scoring
- free users do not receive AI apply analysis
- free users do not receive AI cover generation
- paid or privileged users can access AI features
- AI scoring and AI apply analysis require a temporary CV
- if no CV is loaded, the bot asks for CV first and does not spend LLM tokens

Current Shortlist Rules
-----------------------

Free preview:

- basic lead cards only
- no AI score
- no AI apply analysis

Paid or privileged shortlist:

- per-lead AI score shown in the card
- collected date shown in the card
- explicit `Apply analysis: /apply N` hint shown per lead

Resume Handling
---------------

Implemented:

- `/cv` text input
- PDF, TXT, and MD parsing from Telegram file upload
- temporary in-memory storage only
- `/forgetcv` to wipe temporary resume text

Not implemented:

- permanent resume profile storage
- automatic CV syncing to long-term DB

Scanning Status
---------------

Works in public snapshot:

- public job boards and APIs
- Hacker News `Who is Hiring`
- Workana
- Freelancermap
- Reddit gig scan

Optional or session-dependent:

- Upwork
- Telegram source scanning
- LinkedIn jobs
- LinkedIn posts

What Is Deliberately Out Of Scope Here
--------------------------------------

- final platform submit automation
- blind auto-apply across third-party sites
- private credentials
- private runtime data dumps

Main Files
----------

- `README.md`
- `config/offers.yaml`
- `scripts/run_offer_pipeline.py`
- `scripts/telegram_paid_bot.py`
- `scripts/run_bot_stack.py`
- `src/apply_assistant.py`
- `src/offer_feed.py`
- `src/telegram_paid_store.py`
