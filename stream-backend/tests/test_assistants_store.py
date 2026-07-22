"""Unit tests for the folder-backed AssistantStore."""
from __future__ import annotations

import json

import pytest

from app.assistants import (
    AssistantConfig,
    AssistantExists,
    AssistantNotFound,
    AssistantStore,
    ModelConfig,
    ToolConfig,
    consistent_model,
    default_model_for,
    model_matches_provider,
    slugify,
)


@pytest.fixture
def store(tmp_path):
    return AssistantStore(root=tmp_path)


def test_slugify():
    assert slugify("My Cool Agent!") == "my-cool-agent"
    assert slugify("   ") == "assistant"
    assert slugify("Already-Good_1") == "already-good_1"


def test_create_writes_folder_and_config(store, tmp_path):
    config = AssistantConfig(
        assistant_id="researcher",
        name="Researcher",
        tools=[ToolConfig(name="tavily_search", permission="ask")],
    )
    created = store.create(config)
    assert created.assistant_id == "researcher"

    folder = tmp_path / "researcher"
    assert (folder / "assistant.json").is_file()
    assert (folder / "skills").is_dir()
    assert (folder / "memory").is_dir()

    data = json.loads((folder / "assistant.json").read_text(encoding="utf-8"))
    assert data["name"] == "Researcher"
    assert data["tools"][0]["permission"] == "ask"


def test_create_duplicate_raises(store):
    config = AssistantConfig(assistant_id="dup", name="Dup")
    store.create(config)
    with pytest.raises(AssistantExists):
        store.create(AssistantConfig(assistant_id="dup", name="Dup2"))


def test_get_missing_raises(store):
    with pytest.raises(AssistantNotFound):
        store.get("nope")


def test_update_preserves_created_at(store):
    created = store.create(AssistantConfig(assistant_id="a", name="A"))
    updated = store.update("a", AssistantConfig(assistant_id="a", name="A renamed"))
    assert updated.name == "A renamed"
    assert updated.created_at == created.created_at


def test_save_is_upsert(store):
    store.save(AssistantConfig(assistant_id="up", name="Up"))
    assert store.exists("up")
    store.save(AssistantConfig(assistant_id="up", name="Up2"))
    assert store.get("up").name == "Up2"


def test_delete(store):
    store.create(AssistantConfig(assistant_id="gone", name="Gone"))
    store.delete("gone")
    assert not store.exists("gone")
    with pytest.raises(AssistantNotFound):
        store.delete("gone")


def test_list_ids_and_list(store):
    store.create(AssistantConfig(assistant_id="b", name="B"))
    store.create(AssistantConfig(assistant_id="a", name="A"))
    assert store.list_ids() == ["a", "b"]
    names = {c.assistant_id for c in store.list()}
    assert names == {"a", "b"}


def test_write_skill_registers_and_persists(store, tmp_path):
    store.create(AssistantConfig(assistant_id="s", name="S"))
    entry = store.write_skill("s", "Web Research", "---\nname: web-research\n---\n# body", "desc")
    assert entry.path == "skills/web-research"
    skill_file = tmp_path / "s" / "skills" / "web-research" / "SKILL.md"
    assert skill_file.is_file()
    # Re-registered without duplication.
    store.write_skill("s", "Web Research", "updated", "desc2")
    config = store.get("s")
    assert len([sk for sk in config.skills if sk.path == "skills/web-research"]) == 1


def test_write_memory_registers_and_persists(store, tmp_path):
    store.create(AssistantConfig(assistant_id="m", name="M"))
    entry = store.write_memory("m", "AGENTS", "# project notes")
    assert entry.path == "memory/AGENTS.md"
    assert (tmp_path / "m" / "memory" / "AGENTS.md").is_file()


def test_active_model_synced_into_palette():
    # Empty palette: the active model is migrated in.
    config = AssistantConfig(
        assistant_id="m",
        name="M",
        model=ModelConfig(provider="bedrock", name="eu.anthropic.claude-sonnet-4-5-20250929-v1:0"),
    )
    assert any(
        m.provider == "bedrock" and m.name == "eu.anthropic.claude-sonnet-4-5-20250929-v1:0"
        for m in config.models
    )
    assert len(config.models) == 1


def test_active_model_not_duplicated_in_palette():
    m1 = ModelConfig(provider="google", name="gemini-2.5-pro")
    m2 = ModelConfig(provider="anthropic", name="claude-sonnet-4-5-20250929")
    config = AssistantConfig(assistant_id="m", name="M", model=m1, models=[m1, m2])
    # m1 already present -> not appended again.
    assert len(config.models) == 2


def test_model_matches_provider():
    assert model_matches_provider("google", "gemini-2.5-pro")
    assert not model_matches_provider("google", "gpt-4o")
    assert model_matches_provider("anthropic", "claude-sonnet-4-5-20250929")
    # A bedrock-shaped id is not a direct-anthropic id.
    assert not model_matches_provider("anthropic", "anthropic.claude-3-5-sonnet-20240620-v1:0")
    assert model_matches_provider("openai", "gpt-4o")
    assert model_matches_provider("bedrock", "eu.anthropic.claude-3-5-sonnet-20240620-v1:0")
    assert model_matches_provider("bedrock", "amazon.nova-pro-v1:0")
    # The exact mismatch that broke a real run: a Google model under bedrock.
    assert not model_matches_provider("bedrock", "gemini-2.5-pro")


def test_consistent_model_replaces_mismatch():
    # A Google name under bedrock is replaced with the bedrock default.
    result = consistent_model("bedrock", "gemini-2.5-pro")
    assert result != "gemini-2.5-pro"
    assert model_matches_provider("bedrock", result)
    # A matching name is preserved.
    assert consistent_model("google", "gemini-2.5-flash") == "gemini-2.5-flash"
    # No name falls back to the provider default.
    assert consistent_model("openai", None) == default_model_for("openai")


def test_seed_pairs_provider_and_model(monkeypatch):
    monkeypatch.setenv("RESEARCH_AGENT_PROVIDER", "bedrock")
    monkeypatch.setenv("RESEARCH_AGENT_MODEL", "gemini-2.5-pro")  # wrong provider
    from app.assistants import default_seed_assistants

    seed = default_seed_assistants()[0]
    assert seed.model.provider == "bedrock"
    assert model_matches_provider("bedrock", seed.model.name)


def test_ensure_seeded_creates_default(store):
    assert store.list_ids() == []
    store.ensure_seeded()
    assert "deep-agent" in store.list_ids()
    # Idempotent.
    store.ensure_seeded()
    assert store.list_ids().count("deep-agent") == 1


def test_ensure_seeded_noop_when_not_empty(store):
    store.create(AssistantConfig(assistant_id="x", name="X"))
    store.ensure_seeded()
    assert store.list_ids() == ["x"]
