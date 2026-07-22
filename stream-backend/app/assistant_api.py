"""REST API for creating and managing folder-backed assistants.

Mounted by :mod:`app.main` via ``app.include_router(assistant_router)``. Every
assistant is a deepagents agent built from its :class:`AssistantConfig`.
"""
from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from .assistant_assist import generate_skill, generate_system_prompt, probe_model
from .assistant_catalog import catalog
from .bedrock_catalog import list_bedrock_models
from .assistants import (
    AssistantConfig,
    AssistantExists,
    AssistantNotFound,
    AssistantStore,
    MCPServerConfig,
    MemoryConfig,
    MiddlewareConfig,
    ModelConfig,
    SkillConfig,
    SubAgentConfig,
    ToolConfig,
    slugify,
)

logger = logging.getLogger("stream_backend.assistant_api")

router = APIRouter(prefix="/assistants", tags=["assistants"])

_store = AssistantStore()
try:
    _store.ensure_seeded()
except Exception:  # pragma: no cover - defensive
    logger.warning("assistant_api.seed_failed", exc_info=True)


def store() -> AssistantStore:
    return _store


# --------------------------------------------------------------------------
# Request/response bodies
# --------------------------------------------------------------------------
class AssistantUpsert(BaseModel):
    """Create/update payload. ``assistant_id`` is optional on create."""

    assistant_id: str | None = None
    name: str
    description: str = ""
    model: ModelConfig = Field(default_factory=ModelConfig)
    models: list[ModelConfig] = Field(default_factory=list)
    system_prompt: str = ""
    tools: list[ToolConfig] = Field(default_factory=list)
    mcp: list[MCPServerConfig] = Field(default_factory=list)
    skills: list[SkillConfig] = Field(default_factory=list)
    memory: list[MemoryConfig] = Field(default_factory=list)
    subagents: list[SubAgentConfig] = Field(default_factory=list)
    middleware: list[MiddlewareConfig] = Field(default_factory=list)
    recursion_limit: int = 50
    metadata: dict[str, Any] = Field(default_factory=dict)

    def to_config(self, assistant_id: str) -> AssistantConfig:
        data = self.model_dump()
        data["assistant_id"] = assistant_id
        return AssistantConfig.model_validate(data)


class SkillDraftRequest(BaseModel):
    name: str
    description: str = ""
    instructions: str = ""
    model: ModelConfig | None = None


class PromptDraftRequest(BaseModel):
    name: str
    description: str = ""
    tools: list[str] = Field(default_factory=list)
    subagents: list[str] = Field(default_factory=list)
    instructions: str = ""
    model: ModelConfig | None = None


class TestModelRequest(BaseModel):
    model: ModelConfig


class SkillWriteRequest(BaseModel):
    name: str
    content: str
    description: str = ""


class MemoryWriteRequest(BaseModel):
    name: str
    content: str


# --------------------------------------------------------------------------
# Catalog + AI-assist (declared before /{assistant_id} so they never collide)
# --------------------------------------------------------------------------
@router.get("/catalog")
async def get_catalog() -> dict[str, Any]:
    return catalog()


@router.get("/catalog/bedrock-models")
def get_bedrock_models() -> dict[str, Any]:
    # Sync handler: FastAPI runs the blocking boto3 calls in a threadpool.
    return list_bedrock_models()


@router.post("/assist/system-prompt")
async def assist_system_prompt(request: PromptDraftRequest) -> dict[str, Any]:
    logger.info("assistant_api.assist.system_prompt name=%s", request.name)
    return generate_system_prompt(
        name=request.name,
        description=request.description,
        tools=request.tools,
        subagents=request.subagents,
        model_config=request.model,
        instructions=request.instructions,
    )


@router.post("/assist/skill")
async def assist_skill(request: SkillDraftRequest) -> dict[str, Any]:
    logger.info("assistant_api.assist.skill name=%s", request.name)
    return generate_skill(
        name=request.name,
        description=request.description,
        model_config=request.model,
        instructions=request.instructions,
    )


@router.post("/assist/test-model")
def test_model(request: TestModelRequest) -> dict[str, Any]:
    # Sync handler: FastAPI runs it in a threadpool, so the blocking model
    # round-trip doesn't stall the event loop.
    logger.info(
        "assistant_api.assist.test_model provider=%s model=%s",
        request.model.provider,
        request.model.name,
    )
    return probe_model(request.model)


# --------------------------------------------------------------------------
# CRUD
# --------------------------------------------------------------------------
@router.get("")
async def list_assistants() -> list[dict[str, Any]]:
    return [config.model_dump(mode="json") for config in store().list()]


@router.post("", status_code=201)
async def create_assistant(payload: AssistantUpsert) -> dict[str, Any]:
    assistant_id = payload.assistant_id or slugify(payload.name)
    config = payload.to_config(assistant_id)
    try:
        created = store().create(config)
    except AssistantExists as exc:
        raise HTTPException(status_code=409, detail=f"Assistant '{assistant_id}' already exists") from exc
    logger.info("assistant_api.create id=%s", assistant_id)
    return created.model_dump(mode="json")


@router.get("/{assistant_id}")
async def get_assistant(assistant_id: str) -> dict[str, Any]:
    try:
        return store().get(assistant_id).model_dump(mode="json")
    except AssistantNotFound as exc:
        raise HTTPException(status_code=404, detail=f"Assistant '{assistant_id}' not found") from exc


@router.put("/{assistant_id}")
async def update_assistant(assistant_id: str, payload: AssistantUpsert) -> dict[str, Any]:
    config = payload.to_config(assistant_id)
    try:
        updated = store().update(assistant_id, config)
    except AssistantNotFound as exc:
        raise HTTPException(status_code=404, detail=f"Assistant '{assistant_id}' not found") from exc
    logger.info("assistant_api.update id=%s", assistant_id)
    return updated.model_dump(mode="json")


@router.delete("/{assistant_id}", status_code=204)
async def delete_assistant(assistant_id: str) -> None:
    try:
        store().delete(assistant_id)
    except AssistantNotFound as exc:
        raise HTTPException(status_code=404, detail=f"Assistant '{assistant_id}' not found") from exc
    logger.info("assistant_api.delete id=%s", assistant_id)


@router.post("/{assistant_id}/skills")
async def write_skill(assistant_id: str, request: SkillWriteRequest) -> dict[str, Any]:
    try:
        entry = store().write_skill(assistant_id, request.name, request.content, request.description)
    except AssistantNotFound as exc:
        raise HTTPException(status_code=404, detail=f"Assistant '{assistant_id}' not found") from exc
    return entry.model_dump(mode="json")


@router.post("/{assistant_id}/memory")
async def write_memory(assistant_id: str, request: MemoryWriteRequest) -> dict[str, Any]:
    try:
        entry = store().write_memory(assistant_id, request.name, request.content)
    except AssistantNotFound as exc:
        raise HTTPException(status_code=404, detail=f"Assistant '{assistant_id}' not found") from exc
    return entry.model_dump(mode="json")
