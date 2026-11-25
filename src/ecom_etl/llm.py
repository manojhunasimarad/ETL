"""One way to call a language model, wherever it runs.

Two kinds of backend, chosen with ``ECOM_LLM_PROVIDER`` (or ``--llm`` on the
command line):

``gemini`` (default)
    Google's Gemini API with your AI Studio key. Nothing runs on your laptop,
    so it is the right default for a 4 GB GPU and 8 GB of RAM. The free tier
    limits requests per minute and per day, which is why ``enrich`` packs many
    reviews into one request.

``ollama`` / ``openai``
    Any server that speaks the OpenAI chat-completions API: Ollama, llama.cpp's
    ``llama-server``, vLLM. The same code then runs against a model on Kaggle's
    free T4s, on the college V100 through an SSH tunnel, or (if you accept the
    slowdown) on your laptop. ``ollama`` is ``openai`` with Ollama's address and
    a default model filled in.

Both return ``(text, usage)`` from ``generate_text`` and a parsed dict from
``generate_json``, with usage normalised to ``{"totalTokenCount": n}`` so the
callers count tokens the same way. No SDKs: plain HTTPS with ``urllib``, so the
only dependency stays pandas.
"""

from __future__ import annotations

import json
import os
import re
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Optional, Tuple

from . import config
from .ask import Gemini, LlmError, Transport, _urlopen, api_key, load_dotenv

PROVIDERS = ("gemini", "ollama", "openai")
OLLAMA_URL = "http://localhost:11434/v1"
# Instruction-tuned, good at Portuguese, no "thinking" phase to pay for, 4.7 GB at Q4:
# fits a T4 or the V100 easily. On a 4 GB laptop GPU use qwen2.5:3b-instruct instead.
OLLAMA_MODEL = "qwen2.5:7b-instruct"


def parse_json(text: str) -> dict:
    """The JSON object in a model's reply, tolerating ``` fences and <think> blocks."""
    cleaned = re.sub(r"<think>.*?</think>", "", text or "", flags=re.DOTALL).strip()
    cleaned = re.sub(r"^\s*```(?:json)?\s*|\s*```\s*$", "", cleaned)
    try:
        answer = json.loads(cleaned)
    except json.JSONDecodeError:
        start, end = cleaned.find("{"), cleaned.rfind("}")  # some local models add a sentence around it
        try:
            answer = json.loads(cleaned[start:end + 1]) if 0 <= start < end else None
        except json.JSONDecodeError:
            answer = None
    if not isinstance(answer, dict):
        raise LlmError(f"the model did not return a JSON object: {(text or '')[:200]}")
    return answer


@dataclass
class OpenAICompatible:
    """POST {base_url}/chat/completions, the API Ollama, llama.cpp and vLLM all serve."""

    base_url: str
    model: str
    key: Optional[str] = None
    timeout: float = 180.0      # a local model on a busy GPU can be slow on the first call
    retries: int = 2
    temperature: Optional[float] = 0.0
    json_mode: bool = True     # response_format json_object; turn off for servers that reject it
    transport: Transport = _urlopen

    def generate_text(self, system: str, prompt: str) -> Tuple[str, dict]:
        body = {"model": self.model,
                "messages": [{"role": "system", "content": system}, {"role": "user", "content": prompt}]}
        if self.temperature is not None:
            body["temperature"] = self.temperature
        if self.json_mode:
            body["response_format"] = {"type": "json_object"}
        headers = {"Content-Type": "application/json"}
        if self.key:
            headers["Authorization"] = f"Bearer {self.key}"
        request = urllib.request.Request(self.base_url.rstrip("/") + "/chat/completions",
                                         data=json.dumps(body).encode("utf-8"), method="POST", headers=headers)
        for attempt in range(self.retries + 1):
            try:
                raw = self.transport(request, self.timeout)
                break
            except urllib.error.HTTPError as e:
                detail = e.read().decode("utf-8", "replace")[:300] if hasattr(e, "read") else ""
                if e.code in (429, 503) and attempt < self.retries:  # busy: back off and retry
                    time.sleep(5 * (attempt + 1))
                    continue
                hint = {404: f"model '{self.model}' not found on {self.base_url} (ollama pull {self.model}?)"}
                raise LlmError(f"LLM server {e.code}: {hint.get(e.code, 'error')}. {detail}") from e
            except urllib.error.URLError as e:
                raise LlmError(f"LLM server not reachable at {self.base_url}: {e.reason}. Is it running, "
                               "and is the SSH tunnel open?") from e
        reply = json.loads(raw.decode("utf-8"))
        choices = reply.get("choices") or []
        if not choices:
            raise LlmError(f"the server returned no answer: {json.dumps(reply)[:300]}")
        text = choices[0].get("message", {}).get("content") or ""
        usage = reply.get("usage") or {}
        return text, {"totalTokenCount": int(usage.get("total_tokens", 0) or 0)}

    def generate_json(self, system: str, prompt: str) -> Tuple[dict, dict]:
        text, usage = self.generate_text(system, prompt)
        return parse_json(text), usage


class GeminiJson(Gemini):
    """Gemini for any JSON answer (ask's Gemini insists on a "sql" field). Temperature 0: repeatable labels."""

    def generate_json(self, system: str, prompt: str) -> Tuple[dict, dict]:
        text, usage = self.generate_text(system, prompt)
        return parse_json(text), usage


@dataclass
class Settings:
    provider: str = "gemini"
    model: Optional[str] = None
    base_url: Optional[str] = None
    key: Optional[str] = field(default=None, repr=False)

    @classmethod
    def from_env(cls, provider: Optional[str] = None, model: Optional[str] = None,
                 base_url: Optional[str] = None) -> "Settings":
        load_dotenv(config.PROJECT_ROOT / ".env")
        provider = (provider or os.environ.get("ECOM_LLM_PROVIDER") or "gemini").lower()
        if provider not in PROVIDERS:
            raise ValueError(f"unknown LLM provider '{provider}': use one of {', '.join(PROVIDERS)}")
        return cls(provider, model or os.environ.get("ECOM_LLM_MODEL") or None,
                   base_url or os.environ.get("ECOM_LLM_BASE_URL") or None,
                   os.environ.get("ECOM_LLM_API_KEY") or None)


def make_llm(provider: Optional[str] = None, model: Optional[str] = None, base_url: Optional[str] = None,
             purpose: str = "enrich"):
    """A client for the chosen backend. ``purpose="ask"`` keeps Gemini's SQL-shaped answer check."""
    s = Settings.from_env(provider, model, base_url)
    if s.provider == "gemini":
        llm = Gemini(api_key()) if purpose == "ask" else GeminiJson(api_key(), temperature=0.0)
        if s.model:
            llm.model = s.model
        return llm
    if s.provider == "ollama":
        return OpenAICompatible(s.base_url or OLLAMA_URL, s.model or OLLAMA_MODEL, s.key)
    if not s.base_url:
        raise ValueError("provider 'openai' needs a server address: ECOM_LLM_BASE_URL or --base-url "
                         "(for example http://localhost:8080/v1 through the V100 tunnel)")
    return OpenAICompatible(s.base_url, s.model or "default", s.key)


def describe(llm) -> str:
    """'gemini-flash-latest (gemini)' or 'qwen2.5:7b-instruct at http://localhost:11434/v1'."""
    if isinstance(llm, OpenAICompatible):
        return f"{llm.model} at {llm.base_url}"
    return f"{getattr(llm, 'model', 'unknown')} (gemini)"
