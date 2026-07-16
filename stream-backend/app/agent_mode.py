"""Select the deterministic dummy (test) agent vs the live LLM agent.

Testing the stream/UI shouldn't burn LLM calls. The dummy `DeepAgentDemoRunner`
emits the same event shape (two subagents, messages, tool actions, todos) with no
model calls. This module centralizes the "testing vs live" switch so the API
(in-process) and the Celery worker resolve the mode identically.

Env vars (first match wins):
- ``STREAM_BACKEND_TEST_AGENT`` — simple on/off switch. Truthy -> dummy agent.
- ``STREAM_BACKEND_AGENT_MODE`` — ``testing``/``fixture`` (dummy), ``live``/``research``
  (strict live), or ``auto`` (live, falling back to the dummy if the research
  runtime is unavailable). Default ``auto``.
"""
from __future__ import annotations

import os

_TRUTHY = {"1", "true", "yes", "on", "testing", "test", "dummy", "fixture"}
_FIXTURE_MODES = {"fixture", "testing", "test", "dummy"}
_RESEARCH_MODES = {"research", "live"}


def is_test_agent_enabled() -> bool:
    """True when the deterministic dummy agent should be used (no LLM calls)."""
    return os.getenv("STREAM_BACKEND_TEST_AGENT", "").strip().lower() in _TRUTHY


def resolve_agent_mode() -> str:
    """Return the canonical runner mode: ``fixture``, ``research`` or ``auto``."""
    if is_test_agent_enabled():
        return "fixture"
    mode = os.getenv("STREAM_BACKEND_AGENT_MODE", "auto").strip().lower()
    if mode in _FIXTURE_MODES:
        return "fixture"
    if mode in _RESEARCH_MODES:
        return "research"
    return "auto"
