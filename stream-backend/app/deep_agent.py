from __future__ import annotations

import asyncio
import logging
from copy import deepcopy
from typing import Any

from .models import Checkpoint, RunRecord, ThreadState, new_id, now_iso
from .projections import build_run_snapshot
from .store import Repository

logger = logging.getLogger("stream_backend.fixture")

# Deterministic todos the dummy agent streams (mirrors the real agent's plan
# panel) so the UI can be exercised without any LLM calls.
DUMMY_TODOS_PLANNED = [
    {"id": "todo-1", "content": "Search the web for protocol risks", "status": "in_progress"},
    {"id": "todo-2", "content": "Inspect the sample dataset", "status": "pending"},
]
DUMMY_TODOS_DONE = [
    {"id": "todo-1", "content": "Search the web for protocol risks", "status": "completed"},
    {"id": "todo-2", "content": "Inspect the sample dataset", "status": "completed"},
]


def human_message(content: str, message_id: str | None = None) -> dict[str, Any]:
    return {
        "id": message_id or new_id(),
        "type": "human",
        "content": content,
        "additional_kwargs": {},
        "response_metadata": {},
    }


def ai_message(
    content: str,
    message_id: str | None = None,
    tool_calls: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    message: dict[str, Any] = {
        "id": message_id or new_id(),
        "type": "ai",
        "content": content,
        "additional_kwargs": {},
        "response_metadata": {},
    }
    if tool_calls is not None:
        message["tool_calls"] = tool_calls
    return message


def tool_message(
    content: str,
    tool_call_id: str,
    message_id: str | None = None,
    artifact: Any = None,
) -> dict[str, Any]:
    message: dict[str, Any] = {
        "id": message_id or new_id(),
        "type": "tool",
        "content": content,
        "tool_call_id": tool_call_id,
        "status": "success",
        "additional_kwargs": {},
        "response_metadata": {},
    }
    if artifact is not None:
        message["artifact"] = artifact
    return message


def input_text(input_value: Any) -> str:
    if isinstance(input_value, dict):
        raw_messages = input_value.get("messages")
        if isinstance(raw_messages, list):
            for message in reversed(raw_messages):
                if isinstance(message, str):
                    return message
                if isinstance(message, dict):
                    content = message.get("content")
                    if isinstance(content, str):
                        return content
        content = input_value.get("content")
        if isinstance(content, str):
            return content
    if isinstance(input_value, str):
        return input_value
    return "Run the deep-agent demo."


class DeepAgentDemoRunner:
    """Deterministic deep-agent demo matching the repo's protocol-v2 fixture.

    The runner models an orchestrator that calls the deepagents `task` tool
    twice. Each task owns a scoped namespace and runs its own tool. The emitted
    event stream is what the JS SDK expects for root discovery plus lazy scoped
    `useMessages` / `useToolCalls` / `useValues` subscriptions.
    """

    def __init__(self, repo: Repository) -> None:
        self.repo = repo

    async def run(self, run: RunRecord, input_value: Any) -> None:
        logger.info("fixture.run.start thread_id=%s run_id=%s", run.thread_id, run.run_id)
        await self._mark_running(run)
        thread = await self.repo.ensure_thread(run.thread_id, run.assistant_id)
        previous = thread.state
        step = int(previous.metadata.get("step", 0))

        # Run-scoped ids so multiple demo runs on one thread don't collide (the
        # snapshot projection dedupes by message id and keys subagents by the
        # task tool-call id). Use the full run id to guarantee uniqueness.
        rt = run.run_id
        task1, task2 = f"task-1-{rt}", f"task-2-{rt}"

        # Accumulate prior thread messages (like the research agent) so the
        # per-run projection can slice out this run's new messages; otherwise a
        # demo run on a thread that already has runs projects to zero messages.
        previous_values = previous.values if isinstance(previous.values, dict) else {}
        prior_messages = previous_values.get("messages")
        prior_messages = prior_messages if isinstance(prior_messages, list) else []

        user = human_message(input_text(input_value), f"deep-user-input-{rt}")
        logger.info("fixture.run.input_prepared thread_id=%s run_id=%s content_length=%s", run.thread_id, run.run_id, len(user["content"]))
        task_calls = [
            {
                "name": "task",
                "args": {
                    "description": "Search the web for protocol risks",
                    "subagent_type": "researcher",
                },
                "id": task1,
                "type": "tool_call",
            },
            {
                "name": "task",
                "args": {
                    "description": "Inspect the sample dataset",
                    "subagent_type": "data-analyst",
                },
                "id": task2,
                "type": "tool_call",
            },
        ]
        orchestrator_call = ai_message(
            "",
            f"deep-orchestrator-tool-call-{rt}",
            task_calls,
        )
        root_messages = [*prior_messages, user, orchestrator_call]
        step = await self._commit_state(
            run,
            previous,
            {"messages": root_messages},
            step + 1,
            next_nodes=["tools"],
        )
        logger.info("fixture.run.root_state_committed thread_id=%s run_id=%s step=%s", run.thread_id, run.run_id, step)

        # Mirror the real agent's todo stream so the UI's plan panel is exercised.
        await self.repo.append_event(
            run.thread_id,
            "updates",
            {"todos": DUMMY_TODOS_PLANNED, "run_id": run.run_id},
        )

        await self._start_task_tool(run.thread_id, task1, task_calls[0]["args"])
        await self._start_task_tool(run.thread_id, task2, task_calls[1]["args"])

        logger.info("fixture.run.subagents.schedule thread_id=%s run_id=%s count=2", run.thread_id, run.run_id)
        researcher_task = asyncio.create_task(
            self._run_subagent(
                run,
                namespace=[f"tools:{task1}"],
                subagent_name="researcher",
                task_description="Search the web for protocol risks",
                tool_call_id=f"search-{rt}",
                tool_name="search_web",
                tool_input={"query": "protocol risks"},
                tool_output={
                    "results": [
                        "Reconnect handling must replay buffered events.",
                        "Lifecycle terminals must not hide trailing values.",
                    ]
                },
                final_text="Research completed: reconnect and lifecycle handling need coverage.",
            )
        )
        analyst_task = asyncio.create_task(
            self._run_subagent(
                run,
                namespace=[f"tools:{task2}"],
                subagent_name="data-analyst",
                task_description="Inspect the sample dataset",
                tool_call_id=f"query-{rt}",
                tool_name="query_database",
                tool_input={"table": "sample_data"},
                tool_output={"rows": [{"id": 1}, {"id": 2}], "count": 2},
                final_text="Analysis completed: found 2 sample records.",
            )
        )
        researcher_output, analyst_output = await asyncio.gather(
            researcher_task,
            analyst_task,
        )
        logger.info("fixture.run.subagents.complete thread_id=%s run_id=%s", run.thread_id, run.run_id)

        await self._finish_task_tool(run.thread_id, task1, researcher_output)
        await self._finish_task_tool(run.thread_id, task2, analyst_output)

        final = ai_message(
            "Both subagents completed their tasks successfully.",
            f"deep-orchestrator-final-{rt}",
        )
        root_messages = [
            *prior_messages,
            user,
            orchestrator_call,
            tool_message(researcher_output, task1, f"task-1-result-{rt}"),
            tool_message(analyst_output, task2, f"task-2-result-{rt}"),
            final,
        ]
        current = (await self.repo.get_thread(run.thread_id)).state  # type: ignore[union-attr]
        await self.repo.append_event(
            run.thread_id,
            "updates",
            {"todos": DUMMY_TODOS_DONE, "run_id": run.run_id},
        )
        await self._commit_state(
            run,
            current,
            {"messages": root_messages, "todos": DUMMY_TODOS_DONE},
            step + 1,
            next_nodes=[],
        )
        await self._stream_text_message(
            thread_id=run.thread_id,
            namespace=[],
            node="model:orchestrator",
            message_id=f"deep-orchestrator-final-{rt}",
            text=final["content"],
        )

        run.status = "success"
        await self.repo.save_run(run)
        await self._persist_run_snapshot(run)
        await self.repo.append_event(
            run.thread_id,
            "lifecycle",
            {"event": "completed", "run_id": run.run_id},
        )
        logger.info("fixture.run.success thread_id=%s run_id=%s", run.thread_id, run.run_id)

    async def _persist_run_snapshot(self, run: RunRecord) -> None:
        """Project the finished run once and store it for fast retrieval.

        Mirrors the research runner: builds the run-scoped view from the saved
        checkpoint history and writes it to the run-snapshot table so the
        checkpoints endpoint can serve it with a single keyed lookup.
        """
        try:
            history = await self.repo.get_history(run.thread_id, limit=200)
            snapshot = build_run_snapshot(run, history)
            await self.repo.save_run_snapshot(snapshot)
            logger.info(
                "fixture.run_snapshot.saved thread_id=%s run_id=%s checkpoint_id=%s messages=%s subagents=%s",
                run.thread_id,
                run.run_id,
                snapshot.checkpoint_id,
                len(snapshot.messages),
                len(snapshot.subagents),
            )
        except Exception:
            logger.exception(
                "fixture.run_snapshot.failed thread_id=%s run_id=%s",
                run.thread_id,
                run.run_id,
            )

    async def _mark_running(self, run: RunRecord) -> None:
        logger.info("fixture.run.mark_running thread_id=%s run_id=%s", run.thread_id, run.run_id)
        run.status = "running"
        await self.repo.save_run(run)
        await self.repo.append_event(
            run.thread_id,
            "lifecycle",
            {"event": "running", "run_id": run.run_id},
        )

    async def _commit_state(
        self,
        run: RunRecord,
        previous: ThreadState,
        values: dict[str, Any],
        step: int,
        next_nodes: list[str],
        namespace: list[str] | None = None,
    ) -> int:
        checkpoint = Checkpoint(
            thread_id=run.thread_id,
            checkpoint_ns="/".join(namespace or []),
            checkpoint_id=new_id(),
        )
        logger.info(
            "fixture.state.commit thread_id=%s run_id=%s namespace=%s checkpoint_id=%s step=%s next=%s",
            run.thread_id,
            run.run_id,
            namespace or [],
            checkpoint.checkpoint_id,
            step,
            next_nodes,
        )
        state = ThreadState(
            values=deepcopy(values),
            next=next_nodes,
            checkpoint=checkpoint,
            parent_checkpoint=previous.checkpoint,
            metadata={"step": step, "run_id": run.run_id},
            created_at=now_iso(),
            tasks=[],
        )
        if not namespace:
            await self.repo.save_thread_state(run.thread_id, state)
        await self.repo.append_event(
            run.thread_id,
            "checkpoints",
            {
                "id": checkpoint.checkpoint_id,
                "parent_id": previous.checkpoint.checkpoint_id,
                "step": step,
            },
            namespace=namespace or [],
        )
        await self.repo.append_event(
            run.thread_id,
            "values",
            deepcopy(values),
            namespace=namespace or [],
        )
        return step

    async def _start_task_tool(self, thread_id: str, tool_call_id: str, input_value: Any) -> None:
        logger.info("fixture.task_tool.start thread_id=%s tool_call_id=%s", thread_id, tool_call_id)
        await self.repo.append_event(
            thread_id,
            "tools",
            {
                "event": "tool-started",
                "tool_call_id": tool_call_id,
                "tool_name": "task",
                "input": input_value,
            },
            namespace=[f"tools:{tool_call_id}"],
        )

    async def _run_subagent(
        self,
        run: RunRecord,
        namespace: list[str],
        subagent_name: str,
        task_description: str,
        tool_call_id: str,
        tool_name: str,
        tool_input: dict[str, Any],
        tool_output: Any,
        final_text: str,
    ) -> str:
        logger.info(
            "fixture.subagent.start thread_id=%s run_id=%s subagent=%s namespace=%s",
            run.thread_id,
            run.run_id,
            subagent_name,
            namespace,
        )
        await self.repo.append_event(
            run.thread_id,
            "lifecycle",
            {"event": "running", "run_id": run.run_id},
            namespace=namespace,
        )
        messages = [human_message(task_description, f"{subagent_name}-task")]
        thread = await self.repo.get_thread(run.thread_id)
        if thread is None:
            raise RuntimeError("Thread disappeared while running subagent")
        previous = thread.state
        await self._commit_state(
            run,
            previous,
            {"messages": messages, "agent": subagent_name},
            int(previous.metadata.get("step", 0)) + 1,
            next_nodes=["model"],
            namespace=namespace,
        )
        await asyncio.sleep(0.02)
        logger.info("fixture.subagent.tool_start thread_id=%s run_id=%s subagent=%s tool=%s", run.thread_id, run.run_id, subagent_name, tool_name)
        await self.repo.append_event(
            run.thread_id,
            "tools",
            {
                "event": "tool-started",
                "tool_call_id": tool_call_id,
                "tool_name": tool_name,
                "input": tool_input,
            },
            namespace=[*namespace, f"tools:{tool_call_id}"],
        )
        await asyncio.sleep(0.02)
        logger.info("fixture.subagent.tool_finish thread_id=%s run_id=%s subagent=%s tool=%s", run.thread_id, run.run_id, subagent_name, tool_name)
        await self.repo.append_event(
            run.thread_id,
            "tools",
            {
                "event": "tool-finished",
                "tool_call_id": tool_call_id,
                "output": tool_output,
            },
            namespace=[*namespace, f"tools:{tool_call_id}"],
        )
        messages.append(tool_message(str(tool_output), tool_call_id, f"{tool_call_id}-result", tool_output))
        messages.append(ai_message(final_text, f"{subagent_name}-final"))
        await self._stream_text_message(
            thread_id=run.thread_id,
            namespace=[*namespace, f"model:{subagent_name}"],
            node=f"model:{subagent_name}",
            message_id=f"{subagent_name}-final",
            text=final_text,
        )
        thread = await self.repo.get_thread(run.thread_id)
        if thread is None:
            raise RuntimeError("Thread disappeared while committing subagent")
        previous = thread.state
        await self._commit_state(
            run,
            previous,
            {"messages": messages, "agent": subagent_name},
            int(previous.metadata.get("step", 0)) + 1,
            next_nodes=[],
            namespace=namespace,
        )
        await self.repo.append_event(
            run.thread_id,
            "lifecycle",
            {"event": "completed", "run_id": run.run_id},
            namespace=namespace,
        )
        logger.info("fixture.subagent.complete thread_id=%s run_id=%s subagent=%s", run.thread_id, run.run_id, subagent_name)
        return final_text

    async def _finish_task_tool(self, thread_id: str, tool_call_id: str, output: str) -> None:
        logger.info("fixture.task_tool.finish thread_id=%s tool_call_id=%s output_length=%s", thread_id, tool_call_id, len(output))
        await self.repo.append_event(
            thread_id,
            "tools",
            {
                "event": "tool-finished",
                "tool_call_id": tool_call_id,
                "output": output,
            },
            namespace=[f"tools:{tool_call_id}"],
        )

    async def _stream_text_message(
        self,
        thread_id: str,
        namespace: list[str],
        node: str,
        message_id: str,
        text: str,
    ) -> None:
        logger.info(
            "fixture.message.stream_start thread_id=%s message_id=%s node=%s namespace=%s text_length=%s",
            thread_id,
            message_id,
            node,
            namespace,
            len(text),
        )
        await self.repo.append_event(
            thread_id,
            "messages",
            {"event": "message-start", "id": message_id, "role": "ai"},
            namespace=namespace,
            node=node,
        )
        await self.repo.append_event(
            thread_id,
            "messages",
            {"event": "content-block-start", "index": 0, "content": {"type": "text", "text": ""}},
            namespace=namespace,
            node=node,
        )
        for chunk in [text[: max(1, len(text) // 2)], text[max(1, len(text) // 2) :]]:
            if not chunk:
                continue
            await asyncio.sleep(0.01)
            logger.debug(
                "fixture.message.delta thread_id=%s message_id=%s node=%s namespace=%s chunk_length=%s",
                thread_id,
                message_id,
                node,
                namespace,
                len(chunk),
            )
            await self.repo.append_event(
                thread_id,
                "messages",
                {"event": "content-block-delta", "index": 0, "content": {"type": "text", "text": chunk}},
                namespace=namespace,
                node=node,
            )
        await self.repo.append_event(
            thread_id,
            "messages",
            {"event": "content-block-finish", "index": 0, "content": {"type": "text", "text": text}},
            namespace=namespace,
            node=node,
        )
        await self.repo.append_event(
            thread_id,
            "messages",
            {"event": "message-finish", "reason": "stop"},
            namespace=namespace,
            node=node,
        )
        logger.info("fixture.message.stream_complete thread_id=%s message_id=%s", thread_id, message_id)
