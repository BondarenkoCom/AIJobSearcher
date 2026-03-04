from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import requests


class ApplyAssistantError(RuntimeError):
    pass


def _safe(value: Any) -> str:
    return str(value or "").strip()


def _trim(text: str, limit: int) -> str:
    value = _safe(text)
    if len(value) <= limit:
        return value
    if limit <= 3:
        return value[:limit]
    return value[: limit - 3].rstrip() + "..."


def _extract_json(text: str) -> Dict[str, Any]:
    raw = _safe(text)
    if not raw:
        return {}
    try:
        data = json.loads(raw)
        return data if isinstance(data, dict) else {}
    except Exception:
        pass
    match = re.search(r"\{[\s\S]*\}", raw)
    if not match:
        return {}
    try:
        data = json.loads(match.group(0))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _extract_message_content(body: Dict[str, Any]) -> str:
    choices = body.get("choices")
    if isinstance(choices, list) and choices:
        message = choices[0].get("message")
        if isinstance(message, dict):
            content = message.get("content")
            if isinstance(content, str):
                return content
            if isinstance(content, list):
                parts: List[str] = []
                for item in content:
                    if isinstance(item, dict):
                        parts.append(_safe(item.get("text")))
                    else:
                        parts.append(_safe(item))
                return "\n".join([p for p in parts if p]).strip()
    return _safe(body.get("output_text") or body.get("output"))


def _usage_dict(body: Dict[str, Any]) -> Dict[str, int]:
    usage = body.get("usage")
    if not isinstance(usage, dict):
        return {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
    prompt = int(usage.get("prompt_tokens") or usage.get("input_tokens") or 0)
    completion = int(usage.get("completion_tokens") or usage.get("output_tokens") or 0)
    total = int(usage.get("total_tokens") or (prompt + completion))
    return {
        "prompt_tokens": prompt,
        "completion_tokens": completion,
        "total_tokens": total,
    }


def _price_override(model: str, kind: str) -> float:
    env_name = f"LLM_PRICE_{re.sub(r'[^A-Za-z0-9]+', '_', model).upper()}_{kind.upper()}_PER_1M"
    raw = _safe(os.getenv(env_name))
    if not raw:
        return -1.0
    try:
        return float(raw)
    except Exception:
        return -1.0


def estimate_cost_usd(model: str, *, prompt_tokens: int, completion_tokens: int) -> float:
    defaults = {
        # OpenAI pricing page checked on 2026-03-04.
        "gpt-4.1-mini": {"input": 0.80, "output": 3.20},
        "gpt-4.1-nano": {"input": 0.20, "output": 0.80},
        "gpt-4.1": {"input": 2.00, "output": 8.00},
        # xAI pricing page checked on 2026-03-04.
        "grok-4-fast-non-reasoning": {"input": 0.20, "output": 0.50},
        "grok-4-fast-reasoning": {"input": 0.20, "output": 0.50},
        "grok-4": {"input": 3.00, "output": 15.00},
        "grok-3-mini": {"input": 0.30, "output": 0.50},
    }
    key = _safe(model).lower()
    prices = defaults.get(key)
    if prices is None and key == "grok-4-latest":
        prices = defaults.get("grok-4")
    if prices is None:
        return 0.0
    prompt_price = _price_override(key, "input")
    completion_price = _price_override(key, "output")
    if prompt_price < 0:
        prompt_price = float(prices.get("input") or 0.0)
    if completion_price < 0:
        completion_price = float(prices.get("output") or 0.0)
    return round(((prompt_tokens / 1_000_000.0) * prompt_price) + ((completion_tokens / 1_000_000.0) * completion_price), 8)


@dataclass
class LlmUsage:
    provider: str
    model: str
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int
    estimated_cost_usd: float


@dataclass
class JobAnalysis:
    match_score: int
    strengths: List[str]
    weaknesses: List[str]
    pitch: str
    salary_hint: str
    usage: LlmUsage


@dataclass
class CoverLetter:
    text: str
    usage: LlmUsage


class OpenAICompatibleClient:
    def __init__(self, *, provider: str, api_key: str, api_base: str, model: str, timeout_sec: int = 45) -> None:
        self.provider = _safe(provider) or "unknown"
        self.api_key = _safe(api_key)
        self.api_base = _safe(api_base).rstrip("/")
        self.model = _safe(model)
        self.timeout_sec = max(15, int(timeout_sec))
        if not self.api_key:
            raise ApplyAssistantError(f"Missing API key for {self.provider}.")
        if not self.model:
            raise ApplyAssistantError(f"Missing model for {self.provider}.")

    def chat(self, *, messages: List[Dict[str, str]], temperature: float, max_tokens: int, want_json: bool) -> tuple[str, LlmUsage]:
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        payload: Dict[str, Any] = {
            "model": self.model,
            "messages": messages,
            "temperature": float(temperature),
            "max_tokens": int(max_tokens),
            "stream": False,
        }
        if want_json:
            payload["response_format"] = {"type": "json_object"}
        try:
            resp = requests.post(
                f"{self.api_base}/chat/completions",
                headers=headers,
                json=payload,
                timeout=self.timeout_sec,
            )
        except Exception as exc:
            raise ApplyAssistantError(f"{self.provider} request failed: {type(exc).__name__}: {exc}") from exc
        if resp.status_code >= 400:
            raise ApplyAssistantError(f"{self.provider} error {resp.status_code}: {_trim(resp.text, 500)}")
        try:
            body = resp.json()
        except Exception as exc:
            raise ApplyAssistantError(f"{self.provider} returned non-JSON response.") from exc
        content = _extract_message_content(body)
        usage_data = _usage_dict(body)
        usage = LlmUsage(
            provider=self.provider,
            model=self.model,
            prompt_tokens=int(usage_data.get("prompt_tokens") or 0),
            completion_tokens=int(usage_data.get("completion_tokens") or 0),
            total_tokens=int(usage_data.get("total_tokens") or 0),
            estimated_cost_usd=estimate_cost_usd(
                self.model,
                prompt_tokens=int(usage_data.get("prompt_tokens") or 0),
                completion_tokens=int(usage_data.get("completion_tokens") or 0),
            ),
        )
        return content, usage


class ApplyAssistant:
    def __init__(self) -> None:
        self.analyzer = OpenAICompatibleClient(
            provider="xai",
            api_key=_safe(os.getenv("XAI_API_KEY")),
            api_base="https://api.x.ai/v1",
            model=_safe(os.getenv("APPLY_ANALYZER_MODEL")) or "grok-4-fast-non-reasoning",
        )
        self.cover = OpenAICompatibleClient(
            provider="openai",
            api_key=_safe(os.getenv("OPENAI_API_KEY")),
            api_base="https://api.openai.com/v1",
            model=_safe(os.getenv("APPLY_COVER_MODEL")) or "gpt-4.1-mini",
        )

    def analyze_job(
        self,
        *,
        offer_title: str,
        stack_label: str,
        lead: Dict[str, Any],
        resume_text: str = "",
    ) -> JobAnalysis:
        lead_payload = {
            "role": _safe(lead.get("title")),
            "company": _safe(lead.get("company")),
            "location": _safe(lead.get("location")),
            "platform": _safe(lead.get("platform")),
            "lead_type": _safe(lead.get("lead_type")),
            "stack_hits": list(lead.get("stack_hits") or []),
            "snippet": _trim(_safe(lead.get("snippet")), 1800),
            "url": _safe(lead.get("url")),
            "selected_pack": _safe(offer_title),
            "selected_stack": _safe(stack_label) or "Any stack",
        }
        candidate = {
            "resume_text": _trim(resume_text, 6000),
            "resume_provided": bool(_safe(resume_text)),
        }
        system = (
            "You are a precise job application analyst. "
            "Return JSON only and be conservative. "
            "Do not invent experience or certifications."
        )
        user = (
            "Analyze the lead against the candidate context.\n"
            "Return JSON with keys: match_score, strengths, weaknesses, salary_hint, pitch.\n"
            "Rules:\n"
            "- match_score must be an integer 0..100\n"
            "- strengths max 5 short bullets\n"
            "- weaknesses max 3 short bullets\n"
            "- salary_hint may be empty if unknown\n"
            "- pitch max 2 concise sentences\n"
            f"\nLead:\n{json.dumps(lead_payload, ensure_ascii=False)}\n"
            f"\nCandidate:\n{json.dumps(candidate, ensure_ascii=False)}"
        )
        content, usage = self.analyzer.chat(
            messages=[{"role": "system", "content": system}, {"role": "user", "content": user}],
            temperature=0.1,
            max_tokens=600,
            want_json=True,
        )
        data = _extract_json(content)
        score = int(data.get("match_score") or 0)
        score = max(0, min(100, score))
        strengths = [str(x).strip() for x in data.get("strengths") or [] if str(x).strip()][:5]
        weaknesses = [str(x).strip() for x in data.get("weaknesses") or [] if str(x).strip()][:3]
        pitch = _trim(_safe(data.get("pitch")), 400)
        salary_hint = _trim(_safe(data.get("salary_hint")), 120)
        return JobAnalysis(
            match_score=score,
            strengths=strengths,
            weaknesses=weaknesses,
            pitch=pitch,
            salary_hint=salary_hint,
            usage=usage,
        )

    def generate_cover(
        self,
        *,
        offer_title: str,
        stack_label: str,
        lead: Dict[str, Any],
        resume_text: str = "",
    ) -> CoverLetter:
        lead_payload = {
            "role": _safe(lead.get("title")),
            "company": _safe(lead.get("company")),
            "location": _safe(lead.get("location")),
            "platform": _safe(lead.get("platform")),
            "lead_type": _safe(lead.get("lead_type")),
            "stack_hits": list(lead.get("stack_hits") or []),
            "snippet": _trim(_safe(lead.get("snippet")), 2200),
            "url": _safe(lead.get("url")),
            "selected_pack": _safe(offer_title),
            "selected_stack": _safe(stack_label) or "Any stack",
        }
        candidate_context = _trim(resume_text, 7000)
        system = (
            "You write concise, direct job application cover notes. "
            "Keep it professional, confident, and specific. "
            "Do not invent facts that are not in the candidate context."
        )
        user = (
            "Write one concise cover note, maximum 120 words.\n"
            "No markdown. No greeting fluff. No bullet list.\n"
            "If candidate context is missing, keep it honest and slightly generic.\n"
            f"\nLead:\n{json.dumps(lead_payload, ensure_ascii=False)}\n"
            f"\nCandidate context:\n{candidate_context or 'No resume provided. Use only the selected pack and stack context.'}"
        )
        content, usage = self.cover.chat(
            messages=[{"role": "system", "content": system}, {"role": "user", "content": user}],
            temperature=0.4,
            max_tokens=220,
            want_json=False,
        )
        return CoverLetter(text=_trim(content, 1200), usage=usage)
