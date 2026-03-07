from __future__ import annotations

import argparse
import json
import threading
import webbrowser
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parents[1]
WEBAPP_DIR = (ROOT / "ui" / "webapp").resolve()


class SnakeWebAppHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args: Any, directory: str, **kwargs: Any) -> None:
        super().__init__(*args, directory=directory, **kwargs)

    def list_directory(self, path: str):  # type: ignore[override]
        self.send_error(404, "Directory listing is disabled.")
        return None

    def end_headers(self) -> None:  # noqa: D401
        self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
        self.send_header("Pragma", "no-cache")
        self.send_header("Expires", "0")
        super().end_headers()

    def _send_json(self, obj: Any, status: int = 200) -> None:
        data = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _redirect(self, location: str, status: int = 302) -> None:
        self.send_response(status)
        self.send_header("Location", location)
        self.end_headers()

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path or "/"

        if path == "/api/health":
            self._send_json({"ok": True, "webapp_dir": str(WEBAPP_DIR)})
            return

        if path in ("", "/"):
            self._redirect("/snake/")
            return

        if path == "/snake":
            self._redirect("/snake/")
            return

        if path.startswith("/snake/"):
            return super().do_GET()

        self.send_error(404, "Unknown route.")


def main() -> int:
    ap = argparse.ArgumentParser(description="Serve the public Snake Telegram WebApp.")
    ap.add_argument("--host", default="127.0.0.1", help="Bind host (default: 127.0.0.1)")
    ap.add_argument("--port", type=int, default=8790, help="Bind port (default: 8790)")
    ap.add_argument("--no-open", action="store_true", help="Do not open a browser automatically")
    args = ap.parse_args()

    if not WEBAPP_DIR.exists():
        raise SystemExit(f"WebApp directory not found: {WEBAPP_DIR}")

    handler = partial(SnakeWebAppHandler, directory=str(WEBAPP_DIR))
    httpd = ThreadingHTTPServer((args.host, args.port), handler)
    url = f"http://{args.host}:{args.port}/snake/"

    print(f"[snake-webapp] serving {url}")
    print(f"[snake-webapp] root={WEBAPP_DIR}")

    if not args.no_open:
        def _open() -> None:
            try:
                webbrowser.open(url, new=2)
            except Exception:
                pass

        threading.Thread(target=_open, daemon=True).start()

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("[snake-webapp] stopping...")
    finally:
        httpd.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
