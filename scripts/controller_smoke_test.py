import argparse
import asyncio
import json
import os
from pathlib import Path
import sys

import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.activity_db import connect as db_connect, init_db  # noqa: E402
from src.auto_controller import AutoController  # noqa: E402
from src.config import load_config  # noqa: E402
from src.email_sender import load_env_file  # noqa: E402
from src.profile_store import load_profile  # noqa: E402


async def _run(*, controller: AutoController, profile: dict) -> int:
    page_url = "https://www.linkedin.com/jobs/view/0/"
    page_title = "Controller smoke test"
    questions = [
        {
            "question": "Which areas, tools or technologies related to QA do you find interesting or exciting?",
            "q_norm": "which areas tools or technologies related to qa do you find interesting or exciting",
            "type": "textarea",
            "tag": "textarea",
            "options": [],
        }
    ]
    answers = await controller.suggest_answers(
        page_url=page_url,
        page_title=page_title,
        questions=questions,
        profile=profile,
        context={"mode": "smoke_test"},
    )
    if not answers:
        print("[smoke] no answers returned. Probing API for diagnostics...")
        _probe_api(controller)
        return 2
    for k, v in answers.items():
        print(f"[smoke] answer for: {k}\n{v}\n")
    return 0


def _probe_api(controller: AutoController) -> None:
    path = (controller.request_path or "/chat/completions").strip()
    if not path.startswith("/"):
        path = "/" + path
    url = controller.api_base.rstrip("/") + path
    headers = {
        "Authorization": f"Bearer {os.getenv(controller.api_key_env,'').strip()}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": controller.model,
        "temperature": 0,
        "max_tokens": 40,
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": "Return JSON only."},
            {"role": "user", "content": "Output: {\"ok\": true}"},
        ],
    }
    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=min(20, controller.timeout_sec))
        print(f"[smoke] probe url={url} status={resp.status_code}")
        if resp.status_code >= 400:
            text = (resp.text or "").strip()
            print("[smoke] error body (truncated):")
            print(text[:1000])
            return
        try:
            data = resp.json()
        except Exception:
            print("[smoke] response is not JSON:")
            print((resp.text or "")[:1000])
            return
        print("[smoke] response json (truncated):")
        print(json.dumps(data, ensure_ascii=False)[:1200])
    except Exception as e:
        print(f"[smoke] probe failed: {type(e).__name__}: {e}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=str(ROOT / "data" / "out" / "activity.sqlite"))
    ap.add_argument("--model", default="", help="Override controller model for this run")
    ap.add_argument("--enable", action="store_true", help="Force-enable controller for this run")
    args = ap.parse_args()

    load_env_file(ROOT / ".env")
    load_env_file(ROOT / ".env.accounts")

    cfg = load_config(str(ROOT / "config" / "config.yaml"))
    controller = AutoController.from_config(cfg)
    if args.enable:
        controller.enabled = True
    if args.model.strip():
        controller.model = args.model.strip()

    if not controller.enabled:
        print("[smoke] controller is disabled (set controller.enabled=true or pass --enable).")
        return 2
    if not controller.is_ready():
        print(
            "[smoke] controller is enabled but not ready.\n"
            f"  provider={controller.provider}\n"
            f"  expected env={controller.api_key_env}\n"
            "Set the env var in .env and retry."
        )
        return 2

    db = db_connect(Path(args.db))
    try:
        init_db(db)
        profile = load_profile(db)
    finally:
        db.close()

    return asyncio.run(_run(controller=controller, profile=profile))


if __name__ == "__main__":
    raise SystemExit(main())
