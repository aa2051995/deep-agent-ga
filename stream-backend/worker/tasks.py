from __future__ import annotations

import asyncio
import logging
import os
from typing import Any

from app.deep_agent import DeepAgentDemoRunner
from app.models import RunRecord
from app.research_runtime import ResearchDeepAgentRunner, ResearchRuntimeUnavailable
from app.runtime import create_publishing_repository

from .celery_app import celery_app

logger = logging.getLogger("stream_backend.worker.tasks")


class AutoResearchRunner:
    def __init__(self, repo: Any, strict: bool = False) -> None:
        self.research = ResearchDeepAgentRunner(repo)
        self.fixture = DeepAgentDemoRunner(repo)
        self.strict = strict

    async def run(self, run: RunRecord, input_value: Any) -> None:
        try:
            await self.research.run(run, input_value)
        except ResearchRuntimeUnavailable:
            if self.strict:
                raise
            await self.fixture.run(run, input_value)

    async def resume(self, run: RunRecord, resume_value: Any = None) -> None:
        try:
            await self.research.resume(run, resume_value)
        except ResearchRuntimeUnavailable:
            if self.strict:
                raise
            await self.fixture.run(run, run.kwargs.get("input"))


def runner_for_mode(repo: Any) -> Any:
    mode = os.getenv("STREAM_BACKEND_AGENT_MODE", "auto").lower()
    if mode == "fixture":
        return DeepAgentDemoRunner(repo)
    if mode == "research":
        return AutoResearchRunner(repo, strict=True)
    return AutoResearchRunner(repo)


async def update_run_status(repo: Any, thread_id: str, run_id: str, status: str) -> None:
    run = await repo.get_run(thread_id, run_id)
    if run is not None:
        run.status = status
        await repo.save_run(run)
        if status in {"success", "error", "interrupted", "timeout"}:
            await repo.append_event(
                thread_id,
                "lifecycle",
                {"event": status, "run_id": run_id},
            )


async def execute_run_direct(
    run_record: dict[str, Any],
    *,
    action: str,
    input_value: Any = None,
    resume_value: Any = None,
) -> None:
    run = RunRecord.model_validate(run_record)
    thread_id = run.thread_id
    run_id = run.run_id
    
    if run.cancel_requested:
        logger.info("celery.run.cancel_requested thread_id=%s run_id=%s", thread_id, run_id)
        return
    
    repo = create_publishing_repository()
    await repo.setup()
    try:
        await update_run_status(repo, thread_id, run_id, "running")
        runner = runner_for_mode(repo)
        
        try:
            if action == "run":
                await runner.run(run, input_value)
            else:
                runner_resume = getattr(runner, "resume", None)
                if runner_resume is None:
                    await runner.run(run, input_value)
                else:
                    await runner_resume(run, resume_value)
            
            await update_run_status(repo, thread_id, run_id, "success")
            logger.info("celery.run.success thread_id=%s run_id=%s", thread_id, run_id)
            
        except Exception as exc:
            logger.exception("celery.run.failed thread_id=%s run_id=%s error=%s", thread_id, run_id, exc)
            await update_run_status(repo, thread_id, run_id, "error")
            raise
            
    finally:
        await repo.close()


@celery_app.task(name="deep_research.run_agent", bind=True, autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={"max_retries": 3})
def run_agent(self: Any, run_record: dict[str, Any], input_value: Any = None) -> None:
    run_id = run_record.get("run_id", "unknown")
    thread_id = run_record.get("thread_id", "unknown")
    logger.info("celery.task.run.start thread_id=%s run_id=%s task_id=%s", thread_id, run_id, self.request.id)
    
    try:
        asyncio.run(execute_run_direct(run_record, action="run", input_value=input_value))
        logger.info("celery.task.run.complete thread_id=%s run_id=%s task_id=%s", thread_id, run_id, self.request.id)
    except Exception:
        logger.exception("celery.task.run.error thread_id=%s run_id=%s task_id=%s", thread_id, run_id, self.request.id)
        raise


@celery_app.task(name="deep_research.resume_agent", bind=True, autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={"max_retries": 3})
def resume_agent(self: Any, run_record: dict[str, Any], resume_value: Any = None) -> None:
    run_id = run_record.get("run_id", "unknown")
    thread_id = run_record.get("thread_id", "unknown")
    logger.info("celery.task.resume.start thread_id=%s run_id=%s task_id=%s", thread_id, run_id, self.request.id)
    
    try:
        asyncio.run(execute_run_direct(run_record, action="resume", resume_value=resume_value))
        logger.info("celery.task.resume.complete thread_id=%s run_id=%s task_id=%s", thread_id, run_id, self.request.id)
    except Exception:
        logger.exception("celery.task.resume.error thread_id=%s run_id=%s task_id=%s", thread_id, run_id, self.request.id)
        raise


async def recover_stale_runs() -> list[dict[str, Any]]:
    repo = create_publishing_repository()
    await repo.setup()
    recovered = []
    try:
        threads = await repo.list_threads(limit=1000)
        for thread in threads:
            for status in ["pending", "running"]:
                runs = await repo.list_runs(thread.thread_id, limit=100, status=status)
                for run in runs:
                    logger.info(
                        "celery.recover.stale_run thread_id=%s run_id=%s status=%s",
                        run.thread_id,
                        run.run_id,
                        run.status,
                    )
                    run.status = "interrupted"
                    run.metadata = {**run.metadata, "recovered": True, "recovery_reason": "worker_restart"}
                    await repo.save_run(run)
                    await repo.append_event(
                        run.thread_id,
                        "lifecycle",
                        {"event": "interrupted", "run_id": run.run_id, "reason": "worker_restart"},
                    )
                    recovered.append({"thread_id": run.thread_id, "run_id": run.run_id, "previous_status": status})
    finally:
        await repo.close()
    return recovered


def main() -> None:
    """Worker entry point that recovers stale runs before starting."""
    import os
    
    logger.info("worker.start.recover_stale_runs")
    recovered = asyncio.run(recover_stale_runs())
    logger.info("worker.start.recovery_complete count=%s", len(recovered))
    
    from .celery_app import celery_app
    
    celery_app.worker_main(
        [
            "worker",
            "--loglevel=info",
            f"--queues={os.getenv('STREAM_BACKEND_CELERY_QUEUE', 'deep-research-runs')}",
        ]
    )
