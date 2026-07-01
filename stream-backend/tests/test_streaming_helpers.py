import unittest

from app.main import project_run_checkpoints, sdk_event_name
from app.models import Checkpoint, EventParams, ProtocolEvent, RunRecord, ThreadState


def event(seq: int, method: str, data: dict, namespace: list[str] | None = None) -> ProtocolEvent:
    return ProtocolEvent(
        event_id=f"event-{seq}",
        seq=seq,
        method=method,
        params=EventParams(namespace=namespace or [], data=data),
    )


class StreamingHelperTests(unittest.TestCase):
    def test_sdk_event_name_includes_namespace(self) -> None:
        protocol_event = event(1, "updates", {"run_id": "run-1"}, ["tools:call-1"])

        self.assertEqual(sdk_event_name(protocol_event), "updates|tools:call-1")
        self.assertEqual(sdk_event_name(protocol_event, "metadata"), "metadata|tools:call-1")

    def test_project_run_checkpoints_uses_task_calls_as_subagents(self) -> None:
        previous = ThreadState(
            values={"messages": [{"id": "old", "type": "human", "content": "old"}]},
            checkpoint=Checkpoint(thread_id="thread-1", checkpoint_id="parent"),
            metadata={"step": 1},
        )
        first = ThreadState(
            values={
                "messages": [
                    {"id": "old", "type": "human", "content": "old"},
                    {"id": "human-1", "type": "human", "content": "research this"},
                ]
            },
            checkpoint=Checkpoint(thread_id="thread-1", checkpoint_id="first"),
            parent_checkpoint=previous.checkpoint,
            metadata={"step": 2, "run_id": "run-1"},
        )
        final = ThreadState(
            values={
                "todos": [{"content": "research", "status": "completed"}],
                "messages": [
                    {"id": "old", "type": "human", "content": "old"},
                    {"id": "human-1", "type": "human", "content": "research this"},
                    {
                        "id": "ai-tool",
                        "type": "ai",
                        "content": "",
                        "tool_calls": [
                            {
                                "id": "task-1",
                                "name": "task",
                                "args": {"subagent_type": "researcher", "description": "Find sources"},
                            }
                        ],
                    },
                    {"id": "tool-1", "type": "tool", "tool_call_id": "task-1", "content": "Sources found"},
                    {"id": "ai-final", "type": "ai", "content": "Final answer"},
                ],
            },
            checkpoint=Checkpoint(thread_id="thread-1", checkpoint_id="final"),
            parent_checkpoint=first.checkpoint,
            metadata={"step": 3, "run_id": "run-1"},
        )
        run = RunRecord(run_id="run-1", thread_id="thread-1", assistant_id="deep-agent", status="success")

        projection = project_run_checkpoints(run, [final, first, previous])

        self.assertEqual([message["id"] for message in projection["messages"]], ["human-1", "ai-final"])
        self.assertEqual(projection["todos"], [{"content": "research", "status": "completed"}])
        self.assertEqual(projection["subagents"][0]["name"], "researcher")
        self.assertEqual(projection["subagents"][0]["messages"][1]["content"], "Sources found")

    def test_project_run_checkpoints_skips_same_run_parent_when_resumed(self) -> None:
        previous = ThreadState(
            values={
                "messages": [
                    {"id": "old-human", "type": "human", "content": "old"},
                    {
                        "id": "old-ai-tool",
                        "type": "ai",
                        "content": "",
                        "tool_calls": [
                            {
                                "id": "old-task",
                                "name": "task",
                                "args": {"subagent_type": "old", "description": "old task"},
                            }
                        ],
                    },
                    {"id": "old-tool", "type": "tool", "tool_call_id": "old-task", "content": "old output"},
                    {"id": "old-ai", "type": "ai", "content": "old final"},
                ]
            },
            checkpoint=Checkpoint(thread_id="thread-1", checkpoint_id="previous"),
            metadata={"step": 1, "run_id": "old-run"},
        )
        first = ThreadState(
            values={
                "messages": [
                    *previous.values["messages"],
                    {"id": "human-1", "type": "human", "content": "new"},
                ]
            },
            checkpoint=Checkpoint(thread_id="thread-1", checkpoint_id="first"),
            parent_checkpoint=previous.checkpoint,
            metadata={"step": 2, "run_id": "run-1"},
        )
        recovered_final = ThreadState(
            values={
                "messages": [
                    *first.values["messages"],
                    {
                        "id": "ai-tool",
                        "type": "ai",
                        "content": "",
                        "tool_calls": [
                            {
                                "id": "task-1",
                                "name": "task",
                                "args": {"subagent_type": "researcher", "description": "new task"},
                            }
                        ],
                    },
                    {"id": "tool-1", "type": "tool", "tool_call_id": "task-1", "content": "new output"},
                    {"id": "ai-final", "type": "ai", "content": "new final"},
                ]
            },
            checkpoint=Checkpoint(thread_id="thread-1", checkpoint_id="recovered-final"),
            parent_checkpoint=first.checkpoint,
            metadata={"step": 3, "run_id": "run-1"},
        )
        run = RunRecord(run_id="run-1", thread_id="thread-1", assistant_id="deep-agent", status="success")

        projection = project_run_checkpoints(run, [recovered_final, first, previous])

        self.assertEqual([message["id"] for message in projection["messages"]], ["human-1", "ai-final"])
        self.assertEqual([subagent["key"] for subagent in projection["subagents"]], ["tools:task-1"])
        self.assertEqual(projection["subagents"][0]["messages"][1]["content"], "new output")

    def test_project_run_checkpoints_dedupes_repeated_task_calls(self) -> None:
        previous = ThreadState(
            values={"messages": []},
            checkpoint=Checkpoint(thread_id="thread-1", checkpoint_id="previous"),
            metadata={"step": 1},
        )
        final = ThreadState(
            values={
                "messages": [
                    {"id": "human-1", "type": "human", "content": "new"},
                    {
                        "id": "ai-tool-1",
                        "type": "ai",
                        "content": "",
                        "tool_calls": [
                            {
                                "id": "task-1",
                                "name": "task",
                                "args": {"subagent_type": "researcher", "description": "new task"},
                            }
                        ],
                    },
                    {
                        "id": "ai-tool-1-duplicate",
                        "type": "ai",
                        "content": "",
                        "tool_calls": [
                            {
                                "id": "task-1",
                                "name": "task",
                                "args": {"subagent_type": "researcher", "description": "new task"},
                            }
                        ],
                    },
                    {"id": "tool-1", "type": "tool", "tool_call_id": "task-1", "content": "new output"},
                    {"id": "ai-final", "type": "ai", "content": "new final"},
                ]
            },
            checkpoint=Checkpoint(thread_id="thread-1", checkpoint_id="final"),
            parent_checkpoint=previous.checkpoint,
            metadata={"step": 2, "run_id": "run-1"},
        )
        run = RunRecord(run_id="run-1", thread_id="thread-1", assistant_id="deep-agent", status="success")

        projection = project_run_checkpoints(run, [final, previous])

        self.assertEqual([subagent["key"] for subagent in projection["subagents"]], ["tools:task-1"])


if __name__ == "__main__":
    unittest.main()
