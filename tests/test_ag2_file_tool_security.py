import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from ag2_research import tools


class AG2FileToolSecurityTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tempdir.name)
        (self.root / "docs").mkdir()
        (self.root / "research_state").mkdir()
        (self.root / "strategy").mkdir()
        (self.root / ".hidden").mkdir()
        (self.root / "docs" / "guide.md").write_text("safe docs", encoding="utf-8")
        (self.root / "research_state" / "report.md").write_text("safe research report", encoding="utf-8")
        (self.root / "strategy" / "sample.py").write_text("VALUE = 1\n", encoding="utf-8")
        (self.root / ".env").write_text("API_KEY=secret", encoding="utf-8")
        (self.root / ".hidden" / "notes.md").write_text("hidden", encoding="utf-8")
        (self.root / "strategy" / "payload.exe").write_bytes(b"not allowed")
        self.root_patch = patch.object(tools, "_PROJECT_ROOT", self.root)
        self.root_patch.start()

    def tearDown(self):
        self.root_patch.stop()
        self.tempdir.cleanup()

    def test_read_research_doc_rejects_traversal_and_env(self):
        for path in ("../.env", ".env", "../strategy/sample.py"):
            with self.subTest(path=path):
                payload = json.loads(tools.read_research_doc(path))
                self.assertIn("error", payload)
                self.assertNotIn("secret", json.dumps(payload))

    def test_read_code_rejects_env_hidden_paths_and_unapproved_extensions(self):
        for path in (".env", "docs/../.env", ".hidden/notes.md", "strategy/payload.exe"):
            with self.subTest(path=path):
                payload = json.loads(tools.read_code(path))
                self.assertIn("error", payload)
                self.assertNotIn("secret", json.dumps(payload))

    def test_legal_docs_and_code_remain_readable(self):
        self.assertEqual("safe docs", json.loads(tools.read_research_doc("guide.md"))["content"])
        self.assertIn("VALUE = 1", json.loads(tools.read_code("strategy/sample.py"))["content"])
        self.assertIn("safe research report", json.loads(tools.read_code("research_state/report.md"))["content"])
        offset_payload = json.loads(tools.read_code("strategy/sample.py", max_lines=1, offset=1))
        self.assertEqual("", offset_payload["content"])
        self.assertEqual(1, offset_payload["offset"])

    def test_list_code_rejects_traversal_and_filters_unsafe_files(self):
        denied = json.loads(tools.list_code("../", "*"))
        self.assertIn("error", denied)

        listed = json.loads(tools.list_code(None, "*"))["files"]
        self.assertIn("strategy\\sample.py", listed)
        self.assertNotIn(".env", listed)
        self.assertNotIn(".hidden\\notes.md", listed)
        self.assertNotIn("strategy\\payload.exe", listed)


if __name__ == "__main__":
    unittest.main()
