**Telegram Bot MVP**

**Recommended Name**
- Display name: `Remote Work Hunter`
- Recommended username: `RemoteWorkHunterBot`
- Backup usernames:
  - `RemoteRoleHunterBot`
  - `JobAndGigHunterBot`

**What This Bot Sells**
- Paid access to filtered remote jobs, gigs, and contract leads.
- The profession layer can be expanded later without changing the bot brand.
- It does not sell "guaranteed jobs".
- It does not auto-apply on behalf of the user.

**BotFather Setup**
- `/setname` -> `Remote Work Hunter`
- `/setdescription` -> `Remote jobs and gigs finder.`
- `/setabouttext` -> `Finds filtered remote jobs, gigs, and contract leads across selected professions.`
- `/setuserpic` -> optional product icon
- `/setcommands` ->
  - `start - Open preview and menu`
  - `today - Get today's shortlist`
  - `plans - See paid plans`
  - `status - Check access status`
  - `terms - Terms and refund info`
  - `support - Payment and bot support`

**Money Model**
- Free preview: first 3 leads
- Paid plan 1: `7-day pass` -> `39 XTR`
- Paid plan 2: `30-day pass` -> `119 XTR`
- Both are one-time Telegram Stars payments in MVP
- Recurring subscriptions can be added later

**Where Stars Payment Is Wired**
- Invoice transport:
  - [telegram_bot_api.py](D:/AIJobSearcher/src/telegram_bot_api.py#L86)
  - `send_invoice(...)`
  - uses `currency="XTR"`
  - uses `provider_token=""`
- Invoice creation in bot flow:
  - [telegram_paid_bot.py](D:/AIJobSearcher/scripts/telegram_paid_bot.py#L264)
  - `_send_plan_invoice(...)`
- Pre-checkout validation:
  - [telegram_paid_bot.py](D:/AIJobSearcher/scripts/telegram_paid_bot.py#L330)
  - `_handle_pre_checkout(...)`
- Successful payment handling:
  - [telegram_paid_bot.py](D:/AIJobSearcher/scripts/telegram_paid_bot.py#L283)
  - `_handle_successful_payment(...)`

**Bot Commands**
- `/start`
  - welcome text
  - preview
  - inline menu
- `/today`
  - if paid: full shortlist
  - if not paid: preview + upsell
- `/plans`
  - shows buttons for Stars payment
- `/status`
  - shows active access end date
- `/terms`
  - terms/refund/support text
- `/support`
  - payment support contact

**Core Buttons**
- `Preview`
- `Today's shortlist`
- `Unlock full shortlist`
- `7-day pass - 39 XTR`
- `30-day pass - 119 XTR`

**Data Needed In Env**
- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_BOT_OFFER=qa_gig_hunter`
- `TELEGRAM_ADMIN_CHAT_ID`
- `TELEGRAM_SUPPORT_HANDLE`
- `TELEGRAM_SUPPORT_TEXT`
- `TELEGRAM_TERMS_URL`
- `TELEGRAM_TERMS_TEXT`
- `TELEGRAM_BOT_PHOTO_URL`

Example env keys are in:
- [.env.accounts.example](D:/AIJobSearcher/.env.accounts.example)

**DB Tables Added**
- `bot_users`
- `bot_subscriptions`
- `bot_payments`
- `bot_delivery_log`

Schema lives in:
- [activity_db.py](D:/AIJobSearcher/src/activity_db.py#L5)

**Files Added For MVP**
- [telegram_paid_bot.py](D:/AIJobSearcher/scripts/telegram_paid_bot.py)
- [telegram_bot_api.py](D:/AIJobSearcher/src/telegram_bot_api.py)
- [telegram_paid_store.py](D:/AIJobSearcher/src/telegram_paid_store.py)
- [offer_feed.py](D:/AIJobSearcher/src/offer_feed.py)

**How To Run Locally**
```powershell
python scripts/run_offer_pipeline.py --offer qa_gig_hunter
python scripts/telegram_paid_bot.py --offer qa_gig_hunter
```

**What Still Needs To Be Done Before Production**
- Put real bot token into env
- Set support and terms text
- Run the offer pipeline on a schedule
- Add Render deploy/start command
- Add manual admin refund process
