import unittest

from app.main import project_run_checkpoints, sdk_event_name, select_run_values, select_run_workflow
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

    def test_select_run_values_keeps_root_todos_from_updates(self) -> None:
        events = [
            event(1, "values", {"messages": ["hello"], "run_id": "run-1"}),
            event(
                2,
                "updates",
                {"todos": [{"content": "plan", "status": "completed"}], "run_id": "run-1"},
            ),
            event(
                3,
                "updates",
                {"todos": [{"content": "ignore", "status": "pending"}], "run_id": "run-1"},
                ["tools:call-1"],
            ),
            event(4, "values", {"messages": ["hello", "done"], "run_id": "run-1"}),
        ]

        values = select_run_values(events, "run-1")

        self.assertEqual(
            values,
            {
                "messages": ["hello", "done"],
                "todos": [{"content": "plan", "status": "completed"}],
            },
        )

    def test_select_run_workflow_keeps_big_steps(self) -> None:
        events = [
            event(1, "lifecycle", {"event": "running", "run_id": "run-1"}),
            event(2, "messages", {"event": "message-start", "run_id": "run-1"}),
            event(
                3,
                "updates",
                {"todos": [{"content": "plan", "status": "in_progress"}], "run_id": "run-1"},
            ),
            event(
                4,
                "tools",
                {
                    "event": "tool-started",
                    "tool_name": "task",
                    "input": {"subagent_type": "researcher", "description": "Research sources"},
                    "run_id": "run-1",
                },
            ),
            event(
                5,
                "messages",
                {"event": "content-block-delta", "content": {"text": "tiny"}, "run_id": "run-1"},
            ),
            event(
                6,
                "messages",
                {"event": "content-block-finish", "content": {"text": "Final answer"}, "run_id": "run-1"},
            ),
            event(7, "lifecycle", {"event": "completed", "run_id": "run-1"}),
        ]

        workflow = select_run_workflow(events)

        self.assertEqual(
            [step["title"] for step in workflow],
            [
                "Run running",
                "Todo progress updated",
                "Started researcher",
                "AI response completed",
                "Run completed",
            ],
        )

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


if __name__ == "__main__":
    unittest.main()
