from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import requests


class TelegramApiError(RuntimeError):
    pass


@dataclass
class TelegramBotApi:
    token: str
    timeout_sec: int = 30

    def __post_init__(self) -> None:
        self.base_url = f"https://api.telegram.org/bot{self.token}"
        self.session = requests.Session()

    def _call(self, method: str, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = f"{self.base_url}/{method}"
        resp = self.session.post(url, json=payload or {}, timeout=self.timeout_sec)
        data = resp.json()
        if not resp.ok or not bool(data.get("ok")):
            raise TelegramApiError(f"{method} failed: HTTP {resp.status_code} {data}")
        return data

    def get_me(self) -> Dict[str, Any]:
        return self._call("getMe").get("result") or {}

    def delete_webhook(self, *, drop_pending_updates: bool = False) -> Dict[str, Any]:
        payload = {"drop_pending_updates": bool(drop_pending_updates)}
        return self._call("deleteWebhook", payload).get("result") or {}

    def get_updates(
        self,
        *,
        offset: int,
        timeout: int = 25,
        allowed_updates: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        payload: Dict[str, Any] = {
            "offset": int(offset),
            "timeout": int(timeout),
        }
        if allowed_updates is not None:
            payload["allowed_updates"] = allowed_updates
        return self._call("getUpdates", payload).get("result") or []

    def send_message(
        self,
        *,
        chat_id: int,
        text: str,
        reply_markup: Optional[Dict[str, Any]] = None,
        disable_web_page_preview: bool = True,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "chat_id": int(chat_id),
            "text": text,
            "disable_web_page_preview": bool(disable_web_page_preview),
        }
        if reply_markup is not None:
            payload["reply_markup"] = reply_markup
        return self._call("sendMessage", payload).get("result") or {}

    def answer_callback_query(
        self,
        *,
        callback_query_id: str,
        text: str = "",
        show_alert: bool = False,
    ) -> None:
        payload: Dict[str, Any] = {
            "callback_query_id": callback_query_id,
            "show_alert": bool(show_alert),
        }
        if text:
            payload["text"] = text
        self._call("answerCallbackQuery", payload)

    def answer_pre_checkout_query(
        self,
        *,
        pre_checkout_query_id: str,
        ok: bool,
        error_message: str = "",
    ) -> None:
        payload: Dict[str, Any] = {
            "pre_checkout_query_id": pre_checkout_query_id,
            "ok": bool(ok),
        }
        if error_message:
            payload["error_message"] = error_message
        self._call("answerPreCheckoutQuery", payload)

    def send_invoice(
        self,
        *,
        chat_id: int,
        title: str,
        description: str,
        payload: str,
        amount_stars: int,
        label: str,
        start_parameter: str,
        photo_url: str = "",
    ) -> Dict[str, Any]:
        prices = [{"label": label, "amount": int(amount_stars)}]
        data: Dict[str, Any] = {
            "chat_id": int(chat_id),
            "title": title[:32],
            "description": description[:255],
            "payload": payload,
            "provider_token": "",
            "currency": "XTR",
            "prices": prices,
            "start_parameter": start_parameter[:64],
        }
        if photo_url:
            data["photo_url"] = photo_url
        return self._call("sendInvoice", data).get("result") or {}

    def set_my_commands(self, commands: List[Dict[str, str]]) -> bool:
        payload = {"commands": commands}
        return bool(self._call("setMyCommands", payload).get("result"))

    def set_my_description(self, description: str) -> bool:
        payload = {"description": description[:512]}
        return bool(self._call("setMyDescription", payload).get("result"))

    def set_my_short_description(self, short_description: str) -> bool:
        payload = {"short_description": short_description[:120]}
        return bool(self._call("setMyShortDescription", payload).get("result"))
