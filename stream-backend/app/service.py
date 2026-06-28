from __future__ import annotations

import asyncio
import logging
import os
from collections import defaultdict
from typing import Any

from .deep_agent import DeepAgentDemoRunner
from .models import (
    ProtocolCommand,
    ProtocolError,
    ProtocolSuccess,
    RunRecord,
    new_id,
)
from .research_runtime import ResearchDeepAgentRunner, ResearchRuntimeUnavailable
from .store import Repository


ACTIVE_RUN_STATUSES = {"pending", "running"}
logger = logging.getLogger("stream_backend.service")


def merge_values(current: Any, update: Any) -> Any:
    if update is None:
        return current
    if isinstance(current, dict) and isinstance(update, dict):
        return {**current, **update}
    return update


class AutoResearchRunner:
    def __init__(self, repo: Repository, strict: bool = False) -> None:
        self.research = ResearchDeepAgentRunner(repo)
        self.fixture = DeepAgentDemoRunner(repo)
        self.strict = strict

    async def run(self, run: RunRecord, input_value: Any) -> None:
        logger.info("runner.auto.start thread_id=%s run_id=%s strict=%s", run.thread_id, run.run_id, self.strict)
        try:
            await self.research.run(run, input_value)
        except ResearchRuntimeUnavailable:
            logger.warning("runner.auto.research_unavailable thread_id=%s run_id=%s", run.thread_id, run.run_id)
            if self.strict:
                raise
            logger.info("runner.auto.fixture_fallback thread_id=%s run_id=%s", run.thread_id, run.run_id)
            await self.fixture.run(run, input_value)
        logger.info("runner.auto.complete thread_id=%s run_id=%s", run.thread_id, run.run_id)

    async def resume(self, run: RunRecord, resume_value: Any = None) -> None:
        logger.info("runner.auto.resume.start thread_id=%s run_id=%s strict=%s", run.thread_id, run.run_id, self.strict)
        try:
            await self.research.resume(run, resume_value)
        except ResearchRuntimeUnavailable:
            logger.warning("runner.auto.resume_unavailable thread_id=%s run_id=%s", run.thread_id, run.run_id)
            if self.strict:
                raise
            await self.fixture.run(run, run.kwargs.get("input"))
        logger.info("runner.auto.resume.complete thread_id=%s run_id=%s", run.thread_id, run.run_id)


class ProtocolService:
    def __init__(self, repo: Repository) -> None:
        self.repo = repo
        mode = os.getenv("STREAM_BACKEND_AGENT_MODE", "auto").lower()
        if mode == "fixture":
            self.runner = DeepAgentDemoRunner(repo)
        elif mode == "research":
            self.runner = AutoResearchRunner(repo, strict=True)
        else:
            self.runner = AutoResearchRunner(repo)
        self.tasks: set[asyncio.Task[None]] = set()
        self.run_tasks: dict[tuple[str, str], asyncio.Task[None]] = {}
        self.thread_locks: defaultdict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        logger.info("service.init mode=%s", mode)

    def _track_task(self, run: RunRecord, task: asyncio.Task[None]) -> None:
        self.tasks.add(task)
        self.run_tasks[(run.thread_id, run.run_id)] = task
        task.add_done_callback(lambda done: self._on_task_done(run.thread_id, run.run_id, done))
        logger.info("service.task_scheduled thread_id=%s run_id=%s active_tasks=%s", run.thread_id, run.run_id, len(self.tasks))

    def _on_task_done(self, thread_id: str, run_id: str, task: asyncio.Task[None]) -> None:
        self.tasks.discard(task)
        self.run_tasks.pop((thread_id, run_id), None)
        try:
            exc = task.exception()
            if exc is not None:
                logger.error(
                    "service.task.failed thread_id=%s run_id=%s",
                    thread_id,
                    run_id,
                    exc_info=(type(exc), exc, exc.__traceback__),
                )
            else:
                logger.info("service.task.complete thread_id=%s run_id=%s remaining_tasks=%s", thread_id, run_id, len(self.tasks))
        except asyncio.CancelledError:
            logger.info("service.task.cancelled thread_id=%s run_id=%s remaining_tasks=%s", thread_id, run_id, len(self.tasks))

    async def handle_command(self, thread_id: str, command: ProtocolCommand) -> ProtocolSuccess | ProtocolError:
        logger.info("command.handle.start thread_id=%s command_id=%s method=%s", thread_id, command.id, command.method)
        try:
            if command.method == "run.start":
                result = await self._run_start(thread_id, command)
                logger.info("command.handle.complete thread_id=%s command_id=%s type=%s", thread_id, command.id, result.type)
                return result
            if command.method == "input.respond":
                result = await self._input_respond(thread_id, command)
                logger.info("command.handle.complete thread_id=%s command_id=%s type=%s", thread_id, command.id, result.type)
                return result
            if command.method == "agent.getTree":
                result = ProtocolSuccess(id=command.id, result={"tree": {"name": "demo", "children": []}})
                logger.info("command.handle.complete thread_id=%s command_id=%s type=%s", thread_id, command.id, result.type)
                return result
            if command.method == "state.get":
                thread = await self.repo.ensure_thread(thread_id)
                result = ProtocolSuccess(id=command.id, result={"state": thread.state.model_dump(), "values": thread.state.values})
                logger.info("command.handle.complete thread_id=%s command_id=%s type=%s", thread_id, command.id, result.type)
                return result
            if command.method == "state.listCheckpoints":
                history = await self.repo.get_history(thread_id, limit=int(command.params.get("limit", 10)))
                result = ProtocolSuccess(id=command.id, result={"checkpoints": [state.checkpoint.model_dump() for state in history]})
                logger.info("command.handle.complete thread_id=%s command_id=%s type=%s", thread_id, command.id, result.type)
                return result
            if command.method == "state.fork":
                result = ProtocolSuccess(id=command.id, result={"thread_id": thread_id})
                logger.info("command.handle.complete thread_id=%s command_id=%s type=%s", thread_id, command.id, result.type)
                return result
            logger.warning("command.handle.unknown thread_id=%s command_id=%s method=%s", thread_id, command.id, command.method)
            return ProtocolError(id=command.id, error="unknown_method", message=f"Unsupported command method: {command.method}")
        except Exception as exc:
            logger.exception("command.handle.failed thread_id=%s command_id=%s method=%s", thread_id, command.id, command.method)
            return ProtocolError(id=command.id, error="command_failed", message=str(exc))

    async def _run_start(self, thread_id: str, command: ProtocolCommand) -> ProtocolSuccess | ProtocolError:
        params = command.params
        assistant_id = str(params.get("assistant_id") or "demo")
        multitask_strategy = params.get("multitaskStrategy") or params.get("multitask_strategy") or "reject"
        logger.info(
            "run.start.request thread_id=%s assistant_id=%s strategy=%s",
            thread_id,
            assistant_id,
            multitask_strategy,
        )
        async with self.thread_locks[thread_id]:
            logger.debug("run.start.lock_acquired thread_id=%s", thread_id)
            await self.repo.ensure_thread(thread_id, assistant_id)
            active_runs = await self.repo.list_runs(thread_id, limit=1, status="running")
            if not active_runs:
                active_runs = await self.repo.list_runs(thread_id, limit=1, status="pending")
            if active_runs:
                active_run = active_runs[0]
                logger.warning(
                    "run.start.rejected_active_run thread_id=%s active_run_id=%s active_status=%s",
                    thread_id,
                    active_run.run_id,
                    active_run.status,
                )
                return ProtocolError(
                    id=command.id,
                    error="run_in_progress",
                    message=(
                        "Thread already has an active run. Join, cancel, or wait "
                        "for the active run before starting another run on this thread."
                    ),
                    meta={
                        "thread_id": thread_id,
                        "active_run_id": active_run.run_id,
                        "active_run_status": active_run.status,
                        "requested_multitask_strategy": multitask_strategy,
                    },
                )

            run = RunRecord(
                run_id=new_id(),
                thread_id=thread_id,
                assistant_id=assistant_id,
                metadata=dict(params.get("metadata") or {}),
                kwargs={"input": params.get("input"), "config": params.get("config")},
                multitask_strategy=multitask_strategy,
            )
            await self.repo.create_run(run)
            logger.info("run.start.created thread_id=%s run_id=%s", thread_id, run.run_id)
        task = asyncio.create_task(self.runner.run(run, params.get("input")))
        self._track_task(run, task)
        return ProtocolSuccess(id=command.id, result={"run_id": run.run_id, "thread_id": thread_id})

    async def resume_run(self, thread_id: str, run_id: str, resume_value: Any = None) -> bool:
        async with self.thread_locks[thread_id]:
            run = await self.repo.get_run(thread_id, run_id)
            if run is None:
                logger.warning("run.resume.not_found thread_id=%s run_id=%s", thread_id, run_id)
                return False
            if run.status not in ACTIVE_RUN_STATUSES:
                logger.info("run.resume.not_active thread_id=%s run_id=%s status=%s", thread_id, run_id, run.status)
                return True
            task = self.run_tasks.get((thread_id, run_id))
            if task is not None and not task.done():
                logger.info("run.resume.already_attached thread_id=%s run_id=%s", thread_id, run_id)
                return True
            logger.warning("run.resume.recovering_detached thread_id=%s run_id=%s", thread_id, run_id)
            run.metadata = {**run.metadata, "recovered": True}
            if resume_value is not None:
                run.kwargs = {**run.kwargs, "resume": resume_value}
            await self.repo.save_run(run)
            runner_resume = getattr(self.runner, "resume", None)
            if runner_resume is None:
                task = asyncio.create_task(self.runner.run(run, run.kwargs.get("input")))
            else:
                task = asyncio.create_task(runner_resume(run, resume_value))
            self._track_task(run, task)
            return True

    async def _input_respond(self, thread_id: str, command: ProtocolCommand) -> ProtocolSuccess | ProtocolError:
        logger.info("input.respond thread_id=%s command_id=%s", thread_id, command.id)
        resume_value = command.params.get("response")
        if resume_value is None and "responses" in command.params:
            responses = command.params["responses"]
            if isinstance(responses, list) and responses:
                first = responses[0]
                resume_value = first.get("value") if isinstance(first, dict) else first
            else:
                resume_value = responses

        active_runs = await self.repo.list_runs(thread_id, limit=1, status="running")
        if not active_runs:
            active_runs = await self.repo.list_runs(thread_id, limit=1, status="pending")
        active_runs = [run for run in active_runs if not run.cancel_requested]
        if not active_runs:
            logger.warning("input.respond.no_active_run thread_id=%s command_id=%s", thread_id, command.id)
            return ProtocolError(
                id=command.id,
                error="no_active_run",
                message="No active run is waiting for input on this thread.",
            )

        run = active_runs[0]
        ok = await self.resume_run(thread_id, run.run_id, resume_value)
        if not ok:
            return ProtocolError(
                id=command.id,
                error="no_such_run",
                message="The active run could not be found.",
            )
        return ProtocolSuccess(
            id=command.id,
            result={"run_id": run.run_id, "thread_id": thread_id, "resumed": True},
        )

    async def cancel_run(self, thread_id: str, run_id: str) -> bool:
        logger.info("run.cancel.request thread_id=%s run_id=%s", thread_id, run_id)
        run = await self.repo.get_run(thread_id, run_id)
        if run is None:
            logger.warning("run.cancel.not_found thread_id=%s run_id=%s", thread_id, run_id)
            return False
        task = self.run_tasks.pop((thread_id, run_id), None)
        if task is not None and not task.done():
            task.cancel()
        run.cancel_requested = True
        run.status = "interrupted"
        await self.repo.save_run(run)
        await self.repo.append_event(
            thread_id,
            "lifecycle",
            {"event": "interrupted", "run_id": run_id, "reason": "cancelled"},
        )
        logger.info("run.cancel.saved thread_id=%s run_id=%s", thread_id, run_id)
        return True
