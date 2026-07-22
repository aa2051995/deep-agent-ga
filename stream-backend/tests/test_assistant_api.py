"""Endpoint tests for the assistant management REST API.

Builds a minimal FastAPI app around the assistant router with a temp-dir store
so we avoid importing the heavy app.main (postgres/celery/event brokers).
"""
from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import app.assistant_api as assistant_api
from app.assistants import AssistantStore


@pytest.fixture
def client(tmp_path, monkeypatch):
    store = AssistantStore(root=tmp_path)
    store.ensure_seeded()
    monkeypatch.setattr(assistant_api, "_store", store)
    api = FastAPI()
    api.include_router(assistant_api.router)
    return TestClient(api)


def test_list_returns_seeded(client):
    response = client.get("/assistants")
    assert response.status_code == 200
    ids = {a["assistant_id"] for a in response.json()}
    assert "deep-agent" in ids


def test_catalog(client):
    response = client.get("/assistants/catalog")
    assert response.status_code == 200
    body = response.json()
    assert any(t["name"] == "tavily_search" for t in body["tools"])
    assert "allow" in body["permissions"]
    google = next(p for p in body["providers"] if p["name"] == "google")
    assert any(m["name"] == "gemini-2.5-pro" for m in google["models"])


def test_create_get_update_delete(client):
    create = client.post(
        "/assistants",
        json={
            "name": "My Agent",
            "description": "does things",
            "tools": [{"name": "think_tool", "permission": "ask"}],
        },
    )
    assert create.status_code == 201, create.text
    assistant_id = create.json()["assistant_id"]
    assert assistant_id == "my-agent"

    got = client.get(f"/assistants/{assistant_id}")
    assert got.status_code == 200
    assert got.json()["tools"][0]["permission"] == "ask"

    updated = client.put(
        f"/assistants/{assistant_id}",
        json={"name": "My Agent v2", "tools": []},
    )
    assert updated.status_code == 200
    assert updated.json()["name"] == "My Agent v2"

    deleted = client.delete(f"/assistants/{assistant_id}")
    assert deleted.status_code == 204
    assert client.get(f"/assistants/{assistant_id}").status_code == 404


def test_create_duplicate_conflict(client):
    client.post("/assistants", json={"name": "Dup"})
    again = client.post("/assistants", json={"assistant_id": "dup", "name": "Dup"})
    assert again.status_code == 409


def test_update_missing_404(client):
    response = client.put("/assistants/ghost", json={"name": "Ghost"})
    assert response.status_code == 404


def test_write_skill_and_memory(client):
    client.post("/assistants", json={"assistant_id": "sk", "name": "Sk"})
    skill = client.post(
        "/assistants/sk/skills",
        json={"name": "Web Research", "content": "---\nname: web-research\n---\nbody", "description": "d"},
    )
    assert skill.status_code == 200
    assert skill.json()["path"] == "skills/web-research"

    memory = client.post("/assistants/sk/memory", json={"name": "AGENTS", "content": "# notes"})
    assert memory.status_code == 200
    assert memory.json()["path"] == "memory/AGENTS.md"

    config = client.get("/assistants/sk").json()
    assert any(s["path"] == "skills/web-research" for s in config["skills"])
    assert any(m["path"] == "memory/AGENTS.md" for m in config["memory"])


def test_assist_system_prompt_template_fallback(client):
    response = client.post(
        "/assistants/assist/system-prompt",
        json={"name": "Helper", "description": "helps", "tools": ["tavily_search"], "subagents": []},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["source"] == "template"
    assert "Helper" in body["content"]


def test_test_model_reports_failure(client, monkeypatch):
    monkeypatch.setattr(
        assistant_api,
        "probe_model",
        lambda model: {"ok": False, "message": f"bad key for {model.provider}"},
    )
    response = client.post(
        "/assistants/assist/test-model",
        json={"model": {"provider": "openai", "name": "gpt-4o", "api_key": "nope"}},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["ok"] is False
    assert "openai" in body["message"]


def test_assist_skill_template_fallback(client):
    response = client.post(
        "/assistants/assist/skill",
        json={"name": "Summarize", "description": "summarize text"},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["source"] == "template"
    assert "name: summarize" in body["content"]
