import json
import unittest

from ag2_research.orchestrator import Orchestrator


class ScriptedAgent:
    def __init__(self, replies):
        self.replies = iter(replies)
        self.calls = []

    def generate_reply(self, messages):
        self.calls.append([dict(message) for message in messages])
        return next(self.replies)


class AG2ToolLoopTests(unittest.TestCase):
    def test_tool_round_budget_matches_stage_audit_workload(self):
        self.assertEqual(40, Orchestrator._tool_round_limit("source_librarian"))
        self.assertEqual(24, Orchestrator._tool_round_limit("alpha_hunter"))
        self.assertEqual(8, Orchestrator._tool_round_limit("falsification_officer"))
        self.assertEqual(8, Orchestrator._tool_round_limit("factor_engineer"))

    def test_tool_call_and_tool_result_are_returned_to_model(self):
        tool_call = {
            "role": "assistant",
            "content": "",
            "tool_calls": [{
                "id": "call-1",
                "type": "function",
                "function": {"name": "kbase_search", "arguments": '{"query":"缩量"}'},
            }],
        }
        tool_result = {
            "role": "tool",
            "content": "source-42",
            "tool_responses": [{
                "tool_call_id": "call-1",
                "role": "tool",
                "content": "source-42",
            }],
        }
        final = {"role": "assistant", "content": "source_brief_id: brief-42"}
        agent = ScriptedAgent([tool_call, tool_result, final])

        reply = Orchestrator._generate_reply_with_tools(agent, "inspect kbase")

        self.assertEqual(final, reply)
        self.assertEqual(tool_call, agent.calls[1][-1])
        self.assertEqual(tool_result, agent.calls[2][-1])

    def test_tool_audit_preserves_exact_kbase_source_ids(self):
        source_id = "d0f6e8ef012996e3274a4bee7f75b9a15ab2fdbfe8482400e7287565fad7be96"
        tool_call = {
            "role": "assistant",
            "content": "",
            "tool_calls": [{
                "id": "call-open",
                "type": "function",
                "function": {
                    "name": "kbase_open",
                    "arguments": json.dumps({"source_id": source_id, "layer": "statements"}),
                },
            }],
        }
        tool_result = {
            "role": "tool",
            "content": "",
            "tool_responses": [{
                "tool_call_id": "call-open",
                "role": "tool",
                "content": json.dumps({
                    "catalog_version": "catalog-test",
                    "source_id": source_id,
                    "title": "chip distribution",
                    "layer": "statements",
                    "content": "source text is hashed, not copied into the audit",
                }),
            }],
        }
        final = {"role": "assistant", "content": "brief_id: brief-42"}
        audit = []

        Orchestrator._generate_reply_with_tools(
            ScriptedAgent([tool_call, tool_result, final]),
            "inspect kbase",
            tool_audit=audit,
        )

        self.assertEqual([source_id], audit[0]["source_ids"])
        self.assertEqual("statements", audit[0]["opened_layer"])
        self.assertEqual("ok", audit[0]["status"])
        self.assertNotIn("source text", str(audit[0]))

    def test_tool_call_round_limit_stops_runaway_agent(self):
        repeated_call = {
            "role": "assistant",
            "content": "",
            "tool_calls": [{
                "id": "call-loop",
                "type": "function",
                "function": {"name": "kbase_search", "arguments": "{}"},
            }],
        }
        agent = ScriptedAgent([repeated_call, repeated_call, repeated_call])

        with self.assertRaisesRegex(RuntimeError, "tool-call round limit"):
            Orchestrator._generate_reply_with_tools(
                agent, "inspect kbase", max_tool_rounds=2
            )

    def test_tool_conversation_history_is_reused_for_a_revision(self):
        history = []
        first = ScriptedAgent([{"role": "assistant", "content": "partial brief"}])
        second = ScriptedAgent([{"role": "assistant", "content": "complete brief"}])

        Orchestrator._generate_reply_with_tools(
            first, "research sources", conversation_history=history,
        )
        Orchestrator._generate_reply_with_tools(
            second, "revise only", conversation_history=history,
        )

        self.assertEqual("research sources", second.calls[0][0]["content"])
        self.assertEqual("partial brief", second.calls[0][1]["content"])
        self.assertEqual("revise only", second.calls[0][2]["content"])


if __name__ == "__main__":
    unittest.main()
