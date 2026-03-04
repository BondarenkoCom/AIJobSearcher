Telegram Bot Product Notes
==========================

Bot Identity
------------

- Display name: `Remote Work Hunter`
- Recommended username: `RemoteWorkHunterBot`

Product Positioning
-------------------

This bot sells access to a filtered remote work feed.

It is not:

- a guaranteed job tool
- a blind auto-apply bot
- a generic chat assistant

It is:

- a profession-based remote work hunter
- a shortlist delivery bot
- a paid AI assist layer on top of that shortlist

Current User Flow
-----------------

1. `/start`
2. Choose profession pack
3. Optionally choose stack
4. Free user gets preview
5. Paid user gets full shortlist
6. Paid user with CV gets AI score per lead and AI apply support

Commands
--------

- `/start`
- `/choose`
- `/stack`
- `/sources`
- `/today`
- `/apply`
- `/cv`
- `/forgetcv`
- `/plans`
- `/status`
- `/terms`
- `/support`
- `/adminstats`

Current Access Rules
--------------------

Free users:

- preview leads only
- no AI score in shortlist
- no AI apply analysis
- no AI cover generation

Paid users:

- full shortlist
- AI score shown per lead
- AI apply analysis
- tailored cover notes

Privileged users:

- admins and free testers bypass payment checks

AI Rules
--------

AI runs only when both conditions are true:

1. the user has access
2. a temporary CV is loaded

If access exists but CV is missing:

- the bot does not spend LLM tokens
- the bot asks the user to upload CV first

Shortlist Card Rules
--------------------

Free card:

- title
- platform
- type
- location
- collected date
- contact
- why it fits
- link
- note that apply analysis is paid

Paid card with CV:

- all of the above
- AI `Score`
- `Apply analysis: /apply N`

Apply Assistant
---------------

`/apply N` returns:

- match score
- strong fit bullets
- gaps to watch
- suggested angle
- optional salary hint

Cover generation returns:

- tailored short cover note
- based on the selected lead
- based on the loaded temporary CV

Resume Handling
---------------

Supported:

- plain text
- PDF
- TXT
- MD

Rules:

- stored only in temporary bot memory
- not written to long-term DB storage
- user can clear it with `/forgetcv`

Money Model
-----------

Telegram Stars plans in current MVP:

- `7-day pass` -> `39 XTR`
- `30-day pass` -> `119 XTR`

Current Wiring
--------------

Main files:

- `scripts/telegram_paid_bot.py`
- `src/telegram_bot_api.py`
- `src/telegram_paid_store.py`
- `src/apply_assistant.py`
- `src/offer_feed.py`

Payments:

- Telegram Stars invoices
- one-time access periods

Local Run
---------

```powershell
python scripts/run_offer_pipeline.py --offer qa_gig_hunter
python scripts/telegram_paid_bot.py --offer qa_gig_hunter
```

Combined Background Loop
------------------------

```powershell
python scripts/run_bot_stack.py --default-offer qa_gig_hunter --short-limit 12 --refresh-hours 6
```

What Still Is Not Included
--------------------------

- final third-party platform submit automation
- blind mass auto-apply
- permanent CV profile storage
- mature web dashboard for bot operators
