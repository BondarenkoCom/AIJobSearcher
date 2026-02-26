
Automation toolkit for collecting and tracking remote QA/software-testing opportunities across multiple sources.


- Collects job/project leads from public APIs and web sources.
- Normalizes and stores leads in SQLite.
- Applies deterministic filtering and deduplication.
- Supports optional outreach/apply workflows with rate limits.
- Provides local analytics and a simple UI for monitoring pipeline status.


- Multi-source scanning (job boards, freelance boards, social channels).
- Unified activity DB (`data/out/activity.sqlite`) with event history.
- Safety-first throttling and skip logic.
- Optional model-assisted ranking layer (can be disabled).
- Script-based modular workflow (`scripts/` directory).


- `src/` core modules (DB, routing, controller helpers)
- `scripts/` operational scripts (scan/apply/report/orchestration)
- `config/config.yaml` runtime settings
- `templates/` outreach templates
- `ui/` local UI assets
- `Docs/FREELANCE_AUTOMATION_PATH.md` strategy notes


```powershell
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
python -m playwright install chromium
Copy-Item .env.example .env
Copy-Item .env.accounts.example .env.accounts
```

Then adjust `config/config.yaml` and run a scanner, for example:

```powershell
.\.venv\Scripts\python.exe scripts\upwork_scan_jobs.py --write-db --query "qa automation"
```


- This public version is sanitized.
- Secrets, local sessions, personal documents, and runtime datasets are excluded.
- Keep real credentials only in local `.env` / `.env.accounts` files.


Use the toolkit responsibly and follow platform Terms of Service and local regulations.
