from __future__ import annotations

import asyncio
import logging
import os
from collections import defaultdict
from typing import Any, Protocol, TYPE_CHECKING

from .deep_agent import DeepAgentDemoRunner
from .models import (
    ProtocolCommand,
    ProtocolError,
    ProtocolSuccess,
    RunRecord,
    ThreadState,
    new_id,
)
from .research_runtime import ResearchDeepAgentRunner, ResearchRuntimeUnavailable
from .store import Repository

if TYPE_CHECKING:
    from .streaming import StreamSubscriptionManager


ACTIVE_RUN_STATUSES = {"pending", "running"}
TERMINAL_RUN_STATUSES = {"success", "error", "interrupted", "timeout"}
# Runner backends that select how a run executes. Only "celery" schedules onto a
# worker; anything else (or an unrecognized value) runs in-process via asyncio.
RECOGNIZED_RUNNER_BACKENDS = {"asyncio", "celery"}
logger = logging.getLogger("stream_backend.service")


class RunScheduler(Protocol):
    def enqueue_run(self, run_record: dict[str, Any], input_value: Any = None) -> str: ...
    def enqueue_resume(self, run_record: dict[str, Any], resume_value: Any = None) -> str: ...
    def revoke(self, task_id: str, *, terminate: bool = False) -> None: ...


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
    def __init__(self, repo: Repository, run_scheduler: RunScheduler | None = None) -> None:
        self.repo = repo
        mode = os.getenv("STREAM_BACKEND_AGENT_MODE", "auto").lower()
        if mode == "fixture":
            self.runner = DeepAgentDemoRunner(repo)
        elif mode == "research":
            self.runner = AutoResearchRunner(repo, strict=True)
        else:
            self.runner = AutoResearchRunner(repo)
        raw_backend = (
            os.getenv("STREAM_BACKEND_RUNNER_BACKEND")
            or os.getenv("STREAM_BACKEND_EXECUTION_BACKEND")
            or "asyncio"
        ).lower()
        self.runner_backend = raw_backend
        # Explains, for observability, why a run is (not) scheduled onto a worker.
        self._scheduler_unavailable_reason: str | None = None

        if raw_backend not in RECOGNIZED_RUNNER_BACKENDS:
            logger.warning(
                "service.runner_backend.unrecognized value=%r recognized=%s -> running in-process; "
                "set STREAM_BACKEND_RUNNER_BACKEND=celery to schedule runs on a Celery worker",
                raw_backend,
                sorted(RECOGNIZED_RUNNER_BACKENDS),
            )

        self.run_scheduler = run_scheduler
        if self.run_scheduler is not None:
            pass  # explicitly injected (e.g. tests) — use as-is
        elif raw_backend == "celery":
            try:
                from worker.client import CeleryRunScheduler

                self.run_scheduler = CeleryRunScheduler()
                logger.info(
                    "service.celery_scheduler.enabled queue=%s broker=%s",
                    os.getenv("STREAM_BACKEND_CELERY_QUEUE", "celery"),
                    os.getenv("STREAM_BACKEND_CELERY_BROKER_URL", "<default>"),
                )
            except Exception as exc:
                self._scheduler_unavailable_reason = f"celery scheduler init failed: {exc}"
                logger.exception(
                    "service.celery_scheduler.init_failed -> falling back to in-process asyncio"
                )
        elif raw_backend == "asyncio":
            self._scheduler_unavailable_reason = "runner_backend=asyncio (runs execute in-process by design)"
        else:
            self._scheduler_unavailable_reason = (
                f"runner_backend={raw_backend!r} is not 'celery' "
                "(set STREAM_BACKEND_RUNNER_BACKEND=celery to schedule on the worker)"
            )

        if self.run_scheduler is not None:
            store = os.getenv("STREAM_BACKEND_STORE", "memory").lower()
            broker = os.getenv("STREAM_BACKEND_EVENT_BROKER", "memory").lower()
            if store != "postgres" or broker != "rabbitmq":
                logger.warning(
                    "service.celery_shared_backend_recommended store=%s broker=%s "
                    "expected_store=postgres expected_broker=rabbitmq",
                    store,
                    broker,
                )
        self.tasks: set[asyncio.Task[None]] = set()
        self.run_tasks: dict[tuple[str, str], asyncio.Task[None]] = {}
        self.thread_locks: defaultdict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        logger.info(
            "service.init mode=%s runner_backend=%s execution=%s scheduler=%s%s",
            mode,
            self.runner_backend,
            "celery-worker" if self.run_scheduler is not None else "in-process-asyncio",
            type(self.run_scheduler).__name__ if self.run_scheduler is not None else None,
            "" if self.run_scheduler is not None else f" reason={self._scheduler_unavailable_reason!r}",
        )

    def _track_task(self, run: RunRecord, task: asyncio.Task[None]) -> None:
        self.tasks.add(task)
        self.run_tasks[(run.thread_id, run.run_id)] = task
        task.add_done_callback(lambda done: self._on_task_done(run.thread_id, run.run_id, done))
        logger.info("service.task_scheduled thread_id=%s run_id=%s active_tasks=%s", run.thread_id, run.run_id, len(self.tasks))

    async def _schedule_block_reason(self, run: RunRecord) -> str | None:
        """Return why a run must NOT be (re)scheduled, or None if it may run.

        Guards against double execution: a run that already completed, is already
        enqueued/executing on a worker, or already has a live in-process task must
        not be scheduled again.
        """
        latest = await self.repo.get_run(run.thread_id, run.run_id)
        status = latest.status if latest is not None else run.status
        metadata = latest.metadata if latest is not None else run.metadata

        if status in TERMINAL_RUN_STATUSES:
            return f"run already finished (status={status})"

        task = self.run_tasks.get((run.thread_id, run.run_id))
        if task is not None and not task.done():
            return "run already has a live in-process (asyncio) task"

        if self.run_scheduler is not None:
            celery_task_id = metadata.get("celery_task_id")
            if celery_task_id:
                try:
                    if self.run_scheduler.is_task_active(str(celery_task_id)):
                        return f"run already enqueued/executing on worker (task_id={celery_task_id})"
                except Exception:
                    logger.warning(
                        "service.run.schedule_guard.is_task_active_failed thread_id=%s run_id=%s task_id=%s",
                        run.thread_id,
                        run.run_id,
                        celery_task_id,
                    )
        return None

    async def start_run_task(self, run: RunRecord, input_value: Any) -> bool:
        block_reason = await self._schedule_block_reason(run)
        if block_reason is not None:
            logger.info(
                "service.run.schedule_skipped thread_id=%s run_id=%s reason=%s",
                run.thread_id,
                run.run_id,
                block_reason,
            )
            return False
        if self.run_scheduler is not None:
            try:
                task_id = self.run_scheduler.enqueue_run(run.model_dump(mode="json"), input_value)
            except Exception:
                logger.exception(
                    "service.run.schedule_failed thread_id=%s run_id=%s runner_backend=%s "
                    "-> could not enqueue to worker (is the Celery broker reachable?)",
                    run.thread_id,
                    run.run_id,
                    self.runner_backend,
                )
                raise
            run.metadata = {
                **run.metadata,
                "worker_backend": "celery",
                "celery_task_id": task_id,
                "celery_action": "run",
            }
            await self.repo.save_run(run)
            logger.info(
                "service.run.scheduled_to_worker thread_id=%s run_id=%s task_id=%s queue=%s",
                run.thread_id,
                run.run_id,
                task_id,
                os.getenv("STREAM_BACKEND_CELERY_QUEUE", "celery"),
            )
            return True
        logger.warning(
            "service.run.not_scheduled_to_worker thread_id=%s run_id=%s runner_backend=%s reason=%s "
            "-> executing in-process (asyncio)",
            run.thread_id,
            run.run_id,
            self.runner_backend,
            self._scheduler_unavailable_reason,
        )
        task = self.run_tasks.get((run.thread_id, run.run_id))
        if task is not None and not task.done():
            logger.info(
                "service.task_already_scheduled thread_id=%s run_id=%s",
                run.thread_id,
                run.run_id,
            )
            return False
        task = asyncio.create_task(self.runner.run(run, input_value))
        self._track_task(run, task)
        return True

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

    async def is_run_streaming(self, thread_id: str, run_id: str, run: RunRecord) -> bool:
        """Return True only when there is an active execution task for this run."""
        if run.status not in ACTIVE_RUN_STATUSES:
            return False
        if self.runner_backend == "celery":
            if self.run_scheduler is None:
                logger.warning(
                    "service.is_run_streaming.celery_scheduler_unavailable thread_id=%s run_id=%s",
                    thread_id,
                    run_id,
                )
                return False
            celery_task_id = run.metadata.get("celery_task_id")
            if not celery_task_id:
                logger.warning(
                    "service.is_run_streaming.celery_task_id_missing thread_id=%s run_id=%s",
                    thread_id,
                    run_id,
                )
                return False
            try:
                return self.run_scheduler.is_task_active(celery_task_id)
            except Exception:
                logger.warning(
                    "service.is_run_streaming.celery_check_failed thread_id=%s run_id=%s",
                    thread_id,
                    run_id,
                )
                return False
        task = self.run_tasks.get((thread_id, run_id))
        return task is not None and not task.done()

    async def _latest_run_state(self, thread_id: str, run_id: str) -> ThreadState | None:
        history = await self.repo.get_history(thread_id, limit=200)
        for state in history:
            if state.metadata.get("run_id") == run_id and state.checkpoint.checkpoint_ns in {"", None}:
                return state
        return None

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

    async def _run_start(
        self,
        thread_id: str,
        command: ProtocolCommand,
        *,
        schedule: bool = True,
    ) -> ProtocolSuccess | ProtocolError:
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
            logger.info("run.start.lock_acquired thread_id=%s", thread_id)
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
        if schedule:
            await self.start_run_task(run, params.get("input"))
            logger.info("run.start.scheduled thread_id=%s run_id=%s", thread_id, run.run_id)
        return ProtocolSuccess(id=command.id, result={"run_id": run.run_id, "thread_id": thread_id})

    async def create_pending_run(self, thread_id: str, command: ProtocolCommand) -> ProtocolSuccess | ProtocolError:
        return await self._run_start(thread_id, command, schedule=False)

    async def resume_run(self, thread_id: str, run_id: str, resume_value: Any = None) -> bool:
        async with self.thread_locks[thread_id]:
            run = await self.repo.get_run(thread_id, run_id)
            if run is None:
                logger.warning("run.resume.not_found thread_id=%s run_id=%s", thread_id, run_id)
                return False
            if run.status not in ACTIVE_RUN_STATUSES:
                logger.info("run.resume.not_active thread_id=%s run_id=%s status=%s", thread_id, run_id, run.status)
                return True
            
            celery_task_id = run.metadata.get("celery_task_id")
            if self.run_scheduler is not None and celery_task_id and resume_value is None:
                if self.run_scheduler.is_task_active(celery_task_id):
                    logger.info(
                        "run.resume.celery_task_active thread_id=%s run_id=%s task_id=%s",
                        thread_id,
                        run_id,
                        celery_task_id,
                    )
                    return True
                
                reschedule_count = int(run.metadata.get("reschedule_count", 0))
                max_reschedules = int(os.getenv("STREAM_BACKEND_MAX_RESCHEDULES", "2"))
                
                if reschedule_count >= max_reschedules:
                    logger.warning(
                        "run.resume.reschedule_limit_reached thread_id=%s run_id=%s count=%s max=%s",
                        thread_id,
                        run_id,
                        reschedule_count,
                        max_reschedules,
                    )
                    run.status = "error"
                    run.metadata = {
                        **run.metadata,
                        "error": "reschedule_limit_exceeded",
                        "error_message": f"Run rescheduled {reschedule_count} times without completion",
                    }
                    await self.repo.save_run(run)
                    await self.repo.append_event(
                        thread_id,
                        "lifecycle",
                        {"event": "failed", "run_id": run_id, "reason": "reschedule_limit_exceeded"},
                    )
                    return False
                
                logger.warning(
                    "run.resume.task_dead_rescheduling thread_id=%s run_id=%s task_id=%s reschedule=%s",
                    thread_id,
                    run_id,
                    celery_task_id,
                    reschedule_count + 1,
                )
                run.metadata = {
                    **run.metadata,
                    "reschedule_count": reschedule_count + 1,
                    "rescheduled_at": os.getenv("now_iso", ""),
                    "previous_task_id": celery_task_id,
                }
                await self.repo.save_run(run)
                
            task = self.run_tasks.get((thread_id, run_id))
            if task is not None and not task.done():
                logger.info("run.resume.already_attached thread_id=%s run_id=%s", thread_id, run_id)
                return True
            state = await self._latest_run_state(thread_id, run_id)
            if state is not None and not state.next:
                logger.info("run.resume.mark_completed_from_checkpoint thread_id=%s run_id=%s", thread_id, run_id)
                run.status = "success"
                await self.repo.save_run(run)
                await self.repo.append_event(
                    thread_id,
                    "lifecycle",
                    {"event": "completed", "run_id": run_id, "reason": "checkpoint_complete"},
                )
                return True
            logger.warning("run.resume.recovering_detached thread_id=%s run_id=%s", thread_id, run_id)
            run.metadata = {**run.metadata, "recovered": True}
            if resume_value is not None:
                run.kwargs = {**run.kwargs, "resume": resume_value}
            await self.repo.save_run(run)
            if self.run_scheduler is not None:
                task_id = self.run_scheduler.enqueue_resume(run.model_dump(mode="json"), resume_value)
                run.metadata = {
                    **run.metadata,
                    "worker_backend": "celery",
                    "celery_task_id": task_id,
                    "celery_action": "resume",
                }
                await self.repo.save_run(run)
                logger.info(
                    "run.resume.celery_scheduled thread_id=%s run_id=%s task_id=%s",
                    thread_id,
                    run_id,
                    task_id,
                )
                return True
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
        if self.run_scheduler is not None:
            task_id = run.metadata.get("celery_task_id")
            if isinstance(task_id, str) and task_id:
                terminate = os.getenv("STREAM_BACKEND_CELERY_TERMINATE_ON_CANCEL", "false").lower() in {"1", "true", "yes"}
                self.run_scheduler.revoke(task_id, terminate=terminate)
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
