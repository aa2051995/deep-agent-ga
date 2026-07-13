from __future__ import annotations

import asyncio
import importlib
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from .deep_agent import ai_message, human_message, input_text
from .models import Checkpoint, RunRecord, ThreadState, new_id, now_iso
from .store import Repository

logger = logging.getLogger("stream_backend.research_runtime")


class ResearchRuntimeUnavailable(RuntimeError):
    pass


def ensure_research_agent_import_paths() -> list[str]:
    added: list[str] = []
    candidates = [
        Path(__file__).resolve().parents[1],
        Path(__file__).resolve().parents[2],
    ]
    for path in reversed(candidates):
        if not (path / "research_agent").exists():
            continue
        text = str(path)
        if text not in sys.path:
            sys.path.insert(0, text)
            added.append(text)
    return added


ensure_research_agent_import_paths()


def optional_float_env(name: str) -> float | None:
    value = os.getenv(name)
    if value in {None, ""}:
        return None
    try:
        return float(value)
    except ValueError as exc:
        msg = f"{name} must be a number."
        raise ResearchRuntimeUnavailable(msg) from exc


def bedrock_model_looks_like_id(model: str) -> bool:
    return "." in model or ":" in model or "/" in model


def resolve_bedrock_model_id(
    model: str,
    *,
    region: str | None,
    profile: str | None,
    endpoint_url: str | None,
) -> str:
    if bedrock_model_looks_like_id(model):
        return model
    try:
        import boto3
    except Exception as exc:  # pragma: no cover - environment dependent
        msg = (
            f"Bedrock model '{model}' is a display name, not a model ID. "
            "Install boto3 or set RESEARCH_AGENT_MODEL/AWS_BEDROCK_MODEL_ID "
            "to an exact Bedrock model identifier."
        )
        raise ResearchRuntimeUnavailable(msg) from exc

    try:
        session_kwargs = {"profile_name": profile} if profile else {}
        session = boto3.Session(**session_kwargs)
        client_kwargs: dict[str, str] = {}
        if region:
            client_kwargs["region_name"] = region
        if endpoint_url:
            client_kwargs["endpoint_url"] = endpoint_url
        client = session.client("bedrock", **client_kwargs)
        response = client.list_foundation_models()
    except Exception as exc:  # pragma: no cover - requires AWS access
        msg = (
            f"Bedrock model '{model}' is a display name, not a model ID, and "
            f"the backend could not list Bedrock models to resolve it: {exc}. "
            "Set RESEARCH_AGENT_MODEL or AWS_BEDROCK_MODEL_ID to the exact "
            "Bedrock model identifier from your AWS account/region."
        )
        raise ResearchRuntimeUnavailable(msg) from exc

    normalized = model.casefold().replace(" ", "").replace("-", "")
    matches = []
    for summary in response.get("modelSummaries", []):
        model_id = str(summary.get("modelId") or "")
        model_name = str(summary.get("modelName") or "")
        provider_name = str(summary.get("providerName") or "")
        candidates = [model_id, model_name, f"{provider_name} {model_name}"]
        if any(normalized in candidate.casefold().replace(" ", "").replace("-", "") for candidate in candidates):
            matches.append(model_id)
    matches = [match for match in matches if match]
    if len(matches) == 1:
        logger.info("bedrock.model.resolved display_name=%s model_id=%s", model, matches[0])
        return matches[0]
    if matches:
        msg = (
            f"Bedrock model display name '{model}' matched multiple model IDs: "
            f"{', '.join(matches)}. Set RESEARCH_AGENT_MODEL to the exact ID."
        )
        raise ResearchRuntimeUnavailable(msg)
    msg = (
        f"Bedrock model display name '{model}' was not found in region "
        f"{region or '<default>'}. Set RESEARCH_AGENT_MODEL or AWS_BEDROCK_MODEL_ID "
        "to an exact model identifier available in that AWS region."
    )
    raise ResearchRuntimeUnavailable(msg)


def bedrock_model_kwargs() -> dict[str, Any]:
    raw_model = os.getenv("AWS_BEDROCK_MODEL_ID") or os.getenv("RESEARCH_AGENT_MODEL")
    if not raw_model:
        msg = (
            "RESEARCH_AGENT_PROVIDER=bedrock requires RESEARCH_AGENT_MODEL "
            "or AWS_BEDROCK_MODEL_ID."
        )
        raise ResearchRuntimeUnavailable(msg)

    region = (
        os.getenv("RESEARCH_AGENT_AWS_REGION")
        or os.getenv("AWS_BEDROCK_REGION")
        or os.getenv("AWS_REGION")
        or os.getenv("AWS_DEFAULT_REGION")
    )
    profile = os.getenv("AWS_PROFILE") or os.getenv("AWS_BEDROCK_PROFILE")
    endpoint_url = os.getenv("AWS_BEDROCK_ENDPOINT_URL")
    model = resolve_bedrock_model_id(
        raw_model,
        region=region,
        profile=profile,
        endpoint_url=endpoint_url,
    )
    kwargs: dict[str, Any] = {
        "model": model,
        "temperature": optional_float_env("RESEARCH_AGENT_TEMPERATURE") or 0.0,
    }
    if region:
        kwargs["region_name"] = region
    if profile:
        kwargs["credentials_profile_name"] = profile
    if endpoint_url:
        kwargs["endpoint_url"] = endpoint_url
    max_tokens = optional_float_env("RESEARCH_AGENT_MAX_TOKENS")
    if max_tokens is not None:
        kwargs["max_tokens"] = int(max_tokens)
    return kwargs


def provider_from_env() -> str:
    return os.getenv("RESEARCH_AGENT_PROVIDER", "google").strip().lower()


def namespace_from_metadata(metadata: dict[str, Any] | None) -> list[str]:
    if not metadata:
        return []
    raw = (
        metadata.get("langgraph_checkpoint_ns")
        or metadata.get("checkpoint_ns")
        or metadata.get("namespace")
    )
    if isinstance(raw, list):
        return [str(part) for part in raw]
    if not isinstance(raw, str) or not raw:
        return []
    separator = "|" if "|" in raw else "/"
    return [part for part in raw.split(separator) if part]


def protocol_namespace(namespace: list[str]) -> list[str]:
    for index, part in enumerate(namespace):
        if part.startswith("tools:"):
            return namespace[index:]
    return []


def content_from_chunk(chunk: Any) -> str:
    content = getattr(chunk, "content", None)
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        text = ""
        for block in content:
            if isinstance(block, dict) and block.get("type") == "text":
                text += str(block.get("text") or "")
        return text
    return ""


def is_root_model_stream(event: dict[str, Any]) -> bool:
    if event.get("event") not in {"on_chat_model_stream", "on_llm_stream"}:
        return False
    metadata = event.get("metadata") if isinstance(event.get("metadata"), dict) else {}
    return namespace_from_metadata(metadata) == []


def values_from_update_chunk(chunk: Any) -> dict[str, Any]:
    if not isinstance(chunk, dict):
        return {}
    if "todos" in chunk:
        return {"todos": json_ready(chunk["todos"])}
    merged: dict[str, Any] = {}
    for value in chunk.values():
        if isinstance(value, dict) and "todos" in value:
            merged["todos"] = json_ready(value["todos"])
    return merged


def json_ready(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, list):
        return [json_ready(item) for item in value]
    if isinstance(value, tuple):
        return [json_ready(item) for item in value]
    if isinstance(value, dict):
        return {str(key): json_ready(item) for key, item in value.items()}
    if hasattr(value, "model_dump"):
        return json_ready(value.model_dump())
    if hasattr(value, "dict"):
        return json_ready(value.dict())
    return str(value)


class ResearchDeepAgentRunner:
    """Runs a Python deepagents research agent and mirrors events to the SDK protocol."""

    def __init__(self, repo: Repository) -> None:
        self.repo = repo
        self._agent: Any = None
        self._prompt_mtime: float | None = None
        self._checkpointer_cm: Any = None
        self._checkpointer: Any = None
        self._setup_lock = asyncio.Lock()

    def _current_prompt_mtime(self) -> float | None:
        try:
            import research_agent.prompts as prompts
        except Exception:
            return None
        path = getattr(prompts, "__file__", None)
        if not path:
            return None
        try:
            return Path(path).stat().st_mtime
        except OSError:
            return None

    async def _ensure_agent(self) -> Any:
        async with self._setup_lock:
            prompt_mtime = self._current_prompt_mtime()
            if self._agent is not None and prompt_mtime == self._prompt_mtime:
                logger.info("agent.ensure.reuse")
                return self._agent
            if self._agent is not None:
                logger.info(
                    "agent.ensure.reload prompt_mtime=%s previous_prompt_mtime=%s",
                    prompt_mtime,
                    self._prompt_mtime,
                )

            try:
                logger.info("agent.ensure.imports.start")
                from deepagents import create_deep_agent
                from langchain_google_genai import ChatGoogleGenerativeAI
                from langchain.chat_models import init_chat_model
                import research_agent.prompts as prompts
                from research_agent.tools import tavily_search, think_tool
            except Exception as exc:  # pragma: no cover - environment dependent
                logger.exception("agent.ensure.imports.failed")
                raise ResearchRuntimeUnavailable(str(exc)) from exc
            prompts = importlib.reload(prompts)

            max_concurrent_research_units = int(
                os.getenv("MAX_CONCURRENT_RESEARCH_UNITS", "3")
            )
            max_researcher_iterations = int(
                os.getenv("MAX_RESEARCHER_ITERATIONS", "3")
            )
            current_date = datetime.now().strftime("%Y-%m-%d")
            instructions = (
                prompts.RESEARCH_WORKFLOW_INSTRUCTIONS
                + "\n\n"
                + "=" * 80
                + "\n\n"
                + prompts.SUBAGENT_DELEGATION_INSTRUCTIONS.format(
                    max_concurrent_research_units=max_concurrent_research_units,
                    max_researcher_iterations=max_researcher_iterations,
                )
            )
            research_sub_agent = {
                "name": "research-agent",
                "description": (
                    "Delegate research to the sub-agent researcher. Only give "
                    "this researcher one topic at a time."
                ),
                "system_prompt": prompts.RESEARCHER_INSTRUCTIONS.format(date=current_date),
                "tools": [tavily_search, think_tool],
            }

            provider = provider_from_env()
            logger.info(
                "agent.ensure.configure provider=%s max_units=%s max_iterations=%s",
                provider,
                max_concurrent_research_units,
                max_researcher_iterations,
            )
            if provider == "anthropic":
                model = init_chat_model(
                    model=os.getenv(
                        "RESEARCH_AGENT_MODEL",
                        "anthropic:claude-sonnet-4-5-20250929",
                    ),
                    temperature=0.0,
                )
            elif provider in {"bedrock", "aws-bedrock", "aws_bedrock"}:
                try:
                    from langchain_aws import ChatBedrockConverse
                except Exception as exc:  # pragma: no cover - environment dependent
                    msg = (
                        "Install langchain-aws and configure AWS credentials to use "
                        "RESEARCH_AGENT_PROVIDER=bedrock."
                    )
                    logger.exception("agent.ensure.bedrock_import.failed")
                    raise ResearchRuntimeUnavailable(msg) from exc
                model_kwargs = bedrock_model_kwargs()
                logger.info(
                    "agent.ensure.bedrock.configure model=%s region=%s profile=%s endpoint=%s",
                    model_kwargs.get("model"),
                    model_kwargs.get("region_name"),
                    bool(model_kwargs.get("credentials_profile_name")),
                    bool(model_kwargs.get("endpoint_url")),
                )
                model = ChatBedrockConverse(**model_kwargs)
            else:
                model = ChatGoogleGenerativeAI(
                    model=os.getenv("RESEARCH_AGENT_MODEL", "gemini-2.5-pro"),
                    temperature=0.0,
                    api_key=os.getenv("GOOGLE_API_KEY"),    
                )

            checkpointer = await self._ensure_checkpointer()
            kwargs = {
                "model": model,
                "tools": [tavily_search, think_tool],
                "system_prompt": instructions,
                "subagents": [research_sub_agent],
            }
            if checkpointer is not None:
                kwargs["checkpointer"] = checkpointer
            try:
                logger.info("agent.ensure.create.start checkpointer=%s", checkpointer is not None)
                self._agent = create_deep_agent(**kwargs)
            except TypeError as exc:
                if checkpointer is None or "checkpointer" not in str(exc):
                    raise
                logger.warning("agent.ensure.create.retry_without_checkpointer")
                kwargs.pop("checkpointer", None)
                self._agent = create_deep_agent(**kwargs)
            self._prompt_mtime = prompt_mtime
            logger.info("agent.ensure.create.complete")
            return self._agent

    async def _ensure_checkpointer(self) -> Any | None:
        database_url = (
            os.getenv("STREAM_BACKEND_POSTGRES_URI")
            or os.getenv("POSTGRES_URI")
            or os.getenv("DATABASE_URL")
        )
        if not database_url:
            logger.info("checkpointer.ensure.skipped")
            return None
        if self._checkpointer is not None:
            logger.debug("checkpointer.ensure.reuse")
            return self._checkpointer
        try:
            logger.info("checkpointer.ensure.import.start")
            from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
        except Exception as exc:  # pragma: no cover - environment dependent
            logger.exception("checkpointer.ensure.import.failed")
            raise ResearchRuntimeUnavailable(str(exc)) from exc

        logger.info("checkpointer.ensure.connect.start")
        self._checkpointer_cm = AsyncPostgresSaver.from_conn_string(database_url)
        self._checkpointer = await self._checkpointer_cm.__aenter__()
        await self._checkpointer.setup()
        logger.info("checkpointer.ensure.connect.complete")
        return self._checkpointer

    def _run_config(self, run: RunRecord) -> dict[str, Any]:
        raw_config = run.kwargs.get("config")
        config = raw_config if isinstance(raw_config, dict) else {}
        configurable = config.get("configurable")
        configurable = configurable if isinstance(configurable, dict) else {}
        return {
            **config,
            "configurable": {
                **configurable,
                "thread_id": run.thread_id,
                "checkpoint_ns": configurable.get("checkpoint_ns", ""),
            }
        }

    async def resume(self, run: RunRecord, resume_value: Any = None) -> None:
        logger.info(
            "research.resume.start thread_id=%s run_id=%s has_resume_value=%s",
            run.thread_id,
            run.run_id,
            resume_value is not None,
        )
        if await self._ensure_checkpointer() is None:
            raise ResearchRuntimeUnavailable("Cannot resume a run without a LangGraph checkpointer.")
        agent = await self._ensure_agent()
        run.status = "running"
        await self.repo.save_run(run)
        await self.repo.append_event(
            run.thread_id,
            "lifecycle",
            {"event": "running", "run_id": run.run_id, "recovered": True},
        )
        config = self._run_config(run)
        active_messages: dict[tuple[str, ...], dict[str, Any]] = {}
        final_text = ""
        try:
            input_value: Any = None
            if resume_value is not None:
                try:
                    from langgraph.types import Command
                except Exception as exc:  # pragma: no cover - environment dependent
                    raise ResearchRuntimeUnavailable(str(exc)) from exc
                input_value = Command(resume=resume_value)
            logger.info("research.resume.astream_events.start thread_id=%s run_id=%s", run.thread_id, run.run_id)
            async for event in agent.astream_events(
                input_value,
                config=config,
                version=os.getenv("LANGGRAPH_STREAM_EVENTS_VERSION", "v2"),
            ):
                await self._mirror_agent_event(
                    run.thread_id,
                    run.run_id,
                    event,
                    active_messages,
                )
                if is_root_model_stream(event):
                    final_text += content_from_chunk(event.get("data", {}).get("chunk"))
            logger.info(
                "research.resume.astream_events.complete thread_id=%s run_id=%s final_text_length=%s",
                run.thread_id,
                run.run_id,
                len(final_text),
            )
            await self._save_final_snapshot(run, agent, config, final_text)
            run.status = "success"
            await self.repo.save_run(run)
            await self.repo.append_event(
                run.thread_id,
                "lifecycle",
                {"event": "completed", "run_id": run.run_id, "recovered": True},
            )
            logger.info("research.resume.success thread_id=%s run_id=%s", run.thread_id, run.run_id)
        except Exception as exc:
            logger.exception("research.resume.failed thread_id=%s run_id=%s", run.thread_id, run.run_id)
            run.status = "error"
            await self.repo.save_run(run)
            await self.repo.append_event(
                run.thread_id,
                "lifecycle",
                {"event": "failed", "run_id": run.run_id, "error": str(exc), "recovered": True},
            )

    async def run(self, run: RunRecord, input_value: Any) -> None:
        logger.info("research.run.start thread_id=%s run_id=%s", run.thread_id, run.run_id)
        run.status = "running"
        await self.repo.save_run(run)
        logger.info("research.run.status_running thread_id=%s run_id=%s", run.thread_id, run.run_id)
        await self.repo.append_event(
            run.thread_id,
            "lifecycle",
            {"event": "running", "run_id": run.run_id},
        )
        thread = await self.repo.ensure_thread(run.thread_id, run.assistant_id)
        user = human_message(input_text(input_value))
        logger.info("research.run.input_prepared thread_id=%s run_id=%s content_length=%s", run.thread_id, run.run_id, len(user["content"]))
        previous_values = thread.state.values if isinstance(thread.state.values, dict) else {}
        previous_messages = previous_values.get("messages")
        messages = [*previous_messages, user] if isinstance(previous_messages, list) else [user]
        values: dict[str, Any] = {"messages": messages}
        checkpoint = Checkpoint(thread_id=run.thread_id, checkpoint_id=new_id())
        state = ThreadState(
            values=values,
            next=["agent"],
            checkpoint=checkpoint,
            parent_checkpoint=thread.state.checkpoint,
            metadata={"step": int(thread.state.metadata.get("step", 0)) + 1, "run_id": run.run_id},
            created_at=now_iso(),
            tasks=[],
        )
        await self.repo.save_thread_state(run.thread_id, state)
        logger.info(
            "research.run.initial_state_saved thread_id=%s run_id=%s checkpoint_id=%s step=%s",
            run.thread_id,
            run.run_id,
            checkpoint.checkpoint_id,
            state.metadata["step"],
        )
        await self.repo.append_event(
            run.thread_id,
            "checkpoints",
            {"id": checkpoint.checkpoint_id, "parent_id": thread.state.checkpoint.checkpoint_id, "step": state.metadata["step"], "run_id": run.run_id},
        )
        await self.repo.append_event(run.thread_id, "values", {**values, "run_id": run.run_id})

        config = self._run_config(run)
        input_payload = {"messages": [{"role": "user", "content": user["content"]}]}
        active_messages: dict[tuple[str, ...], dict[str, Any]] = {}
        final_text = ""
        agent = await self._ensure_agent()

        try:
            logger.info("research.run.astream_events.start thread_id=%s run_id=%s", run.thread_id, run.run_id)
            async for event in agent.astream_events(
                input_payload,
                config=config,
                version=os.getenv("LANGGRAPH_STREAM_EVENTS_VERSION", "v2"),
            ):
                await self._mirror_agent_event(
                    run.thread_id,
                    run.run_id,
                    event,
                    active_messages,
                )
                if is_root_model_stream(event):
                    final_text += content_from_chunk(event.get("data", {}).get("chunk"))

            logger.info(
                "research.run.astream_events.complete thread_id=%s run_id=%s final_text_length=%s",
                run.thread_id,
                run.run_id,
                len(final_text),
            )
            await self._save_final_snapshot(run, agent, config, final_text, fallback_messages=messages)
            run.status = "success"
            await self.repo.save_run(run)
            await self.repo.append_event(
                run.thread_id,
                "lifecycle",
                {"event": "completed", "run_id": run.run_id},
            )
            logger.info("research.run.success thread_id=%s run_id=%s", run.thread_id, run.run_id)
        except Exception as exc:
            logger.exception("research.run.failed thread_id=%s run_id=%s", run.thread_id, run.run_id)
            run.status = "error"
            await self.repo.save_run(run)
            await self.repo.append_event(
                run.thread_id,
                "lifecycle",
                {"event": "failed", "run_id": run.run_id, "error": str(exc)},
            )

    async def _save_final_snapshot(
        self,
        run: RunRecord,
        agent: Any,
        config: dict[str, Any],
        final_text: str,
        fallback_messages: list[dict[str, Any]] | None = None,
    ) -> None:
        values: dict[str, Any] | None = None
        if hasattr(agent, "aget_state"):
            logger.info("research.snapshot.aget_state.start thread_id=%s run_id=%s", run.thread_id, run.run_id)
            try:
                snapshot = await agent.aget_state(config)
            except ValueError as exc:
                logger.warning(
                    "research.snapshot.aget_state.unavailable thread_id=%s run_id=%s error=%s",
                    run.thread_id,
                    run.run_id,
                    exc,
                )
            else:
                if getattr(snapshot, "values", None):
                    values = json_ready(dict(snapshot.values))

        if values is None:
            messages = fallback_messages or []
            values = {"messages": [*messages, ai_message(final_text or "Research complete.")]}
        else:
            logger.info("research.snapshot.using_agent_state thread_id=%s run_id=%s", run.thread_id, run.run_id)

        thread = await self.repo.ensure_thread(run.thread_id, run.assistant_id)
        final_checkpoint = Checkpoint(thread_id=run.thread_id, checkpoint_id=new_id())
        final_state = ThreadState(
            values=values,
            next=[],
            checkpoint=final_checkpoint,
            parent_checkpoint=thread.state.checkpoint,
            metadata={"step": int(thread.state.metadata.get("step", 0)) + 1, "run_id": run.run_id},
            created_at=now_iso(),
            tasks=[],
        )
        await self.repo.save_thread_state(run.thread_id, final_state)
        logger.info(
            "research.snapshot.saved thread_id=%s run_id=%s checkpoint_id=%s step=%s",
            run.thread_id,
            run.run_id,
            final_checkpoint.checkpoint_id,
            final_state.metadata["step"],
        )
        await self.repo.append_event(
            run.thread_id,
            "checkpoints",
            {
                "id": final_checkpoint.checkpoint_id,
                "parent_id": thread.state.checkpoint.checkpoint_id,
                "step": final_state.metadata["step"],
                "run_id": run.run_id,
            },
        )
        await self.repo.append_event(run.thread_id, "values", {**values, "run_id": run.run_id})

    async def _mirror_agent_event(
        self,
        thread_id: str,
        run_id: str,
        event: dict[str, Any],
        active_messages: dict[tuple[str, ...], dict[str, Any]],
    ) -> None:
        kind = event.get("event")
        metadata = event.get("metadata") if isinstance(event.get("metadata"), dict) else {}
        namespace = protocol_namespace(namespace_from_metadata(metadata))
        message_key = tuple(namespace)
        node = str(metadata.get("langgraph_node") or event.get("name") or "agent")
        data = event.get("data") if isinstance(event.get("data"), dict) else {}
        # logger.debug(
        #     "research.event.mirror thread_id=%s kind=%s node=%s namespace=%s",
        #     thread_id,
        #     kind,
        #     node,
        #     namespace,
        # )

        if kind in {"on_chat_model_start", "on_llm_start"}:
            message_id = new_id()
            # logger.info("research.event.message_start thread_id=%s message_id=%s node=%s namespace=%s", thread_id, message_id, node, namespace)
            active_messages[message_key] = {"id": message_id, "text": "", "events": 0}
            await self.repo.append_event(
                thread_id,
                "messages",
                {"event": "message-start", "id": message_id, "role": "ai", "run_id": run_id},
                namespace=namespace,
                node=node,
            )
            await self.repo.append_event(
                thread_id,
                "messages",
                {"event": "content-block-start", "index": 0, "content": {"type": "text", "text": ""}, "run_id": run_id},
                namespace=namespace,
                node=node,
            )
            return

        if kind in {"on_chat_model_stream", "on_llm_stream"}:
            # await asyncio.sleep(.9)  # yield to event loop
            text = content_from_chunk(data.get("chunk"))
            if text:
                # logger.debug("research.event.message_delta thread_id=%s node=%s namespace=%s chunk_length=%s", thread_id, node, namespace, len(text))
                current = active_messages.setdefault(
                    message_key,
                    {"id": new_id(), "text": "", "events": 0},
                )
                current["text"] = str(current["text"]) + text
                current["events"] = int(current.get("events", 0)) + 1
                if current["events"] % 10 == 0:
                    logger.info(
                        "research.event.message_delta thread_id=%s run_id=%s node=%s namespace=%s message_id=%s event_count=%s text_length=%s chunk_length=%s",
                        thread_id,
                        run_id,
                        node,
                        namespace,
                        current["id"],
                        current["events"],
                        len(current["text"]),
                        len(text),
                    )
                await self.repo.append_event(
                    thread_id,
                    "messages",
                    {"event": "content-block-delta", "index": 0, "content": {"type": "text", "text": text}, "run_id": run_id},
                    namespace=namespace,
                    node=node,
                )
            return

        if kind in {"on_chat_model_end", "on_llm_end"}:
            current = active_messages.pop(message_key, None)
            logger.info(
                "research.event.message_end thread_id=%s node=%s namespace=%s text_length=%s",
                thread_id,
                node,
                namespace,
                len(current["text"]) if current else 0,
            )
            await self.repo.append_event(
                thread_id,
                "messages",
                {
                    "event": "content-block-finish",
                    "index": 0,
                    "content": {"type": "text", "text": current["text"] if current else ""},
                    "run_id": run_id,
                },
                namespace=namespace,
                node=node,
            )
            await self.repo.append_event(
                thread_id,
                "messages",
                {"event": "message-finish", "reason": "stop", "run_id": run_id},
                namespace=namespace,
                node=node,
            )
            return

        if kind == "on_chain_stream":
            values = values_from_update_chunk(data.get("chunk"))
            if values:
                # logger.info(
                #     "research.event.values_update thread_id=%s run_id=%s namespace=%s keys=%s",
                #     thread_id,
                #     run_id,
                #     namespace,
                #     sorted(values),
                # )
                await self.repo.append_event(
                    thread_id,
                    "updates",
                    {**values, "run_id": run_id},
                    namespace=namespace,
                    node=node,
                )
            return

        if kind == "on_chain_end":
            values = values_from_update_chunk(data.get("output"))
            if values:
                logger.info(
                    "research.event.values_end thread_id=%s run_id=%s namespace=%s keys=%s",
                    thread_id,
                    run_id,
                    namespace,
                    sorted(values),
                )
                await self.repo.append_event(
                    thread_id,
                    "updates",
                    {**values, "run_id": run_id},
                    namespace=namespace,
                    node=node,
                )
            return

        if kind == "on_tool_start":
            logger.info(
                "research.event.tool_start thread_id=%s tool=%s namespace=%s",
                thread_id,
                event.get("name") or "tool",
                namespace,
            )
            await self.repo.append_event(
                thread_id,
                "tools",
                {
                    "event": "tool-started",
                    "tool_call_id": str(event.get("run_id") or new_id()),
                    "tool_name": str(event.get("name") or "tool"),
                    "input": data.get("input"),
                    "run_id": run_id,
                },
                namespace=namespace,
            )
            return

        if kind == "on_tool_end":
            # logger.info(
            #     "research.event.tool_end thread_id=%s tool=%s namespace=%s",
            #     thread_id,
            #     event.get("name") or "tool",
            #     namespace,
            # )
            await self.repo.append_event(
                thread_id,
                "tools",
                {
                    "event": "tool-finished",
                    "tool_call_id": str(event.get("run_id") or new_id()),
                    "output": data.get("output"),
                    "run_id": run_id,
                },
                namespace=namespace,
            )
