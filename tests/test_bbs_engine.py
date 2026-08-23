"""Unit tests for BBS menu engine — temp SQLite, no radio."""
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bbs.engine import BBSEngine
from bbs.store import BBSStore


class BBSEngineTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()
        self.store = BBSStore(self.tmp.name)
        # Simple resolver: "ALICE" -> "!alice01"
        self.engine = BBSEngine(
            self.store,
            node_resolver=lambda t: "!alice01" if t.upper() == "ALICE" else None,
        )
        self.me = "!bob0001"
        self.other = "!alice01"

    def tearDown(self):
        os.unlink(self.tmp.name)

    def _h(self, text, node=None):
        return self.engine.handle(node or self.me, text)

    def test_should_handle_bbs_keyword(self):
        self.assertTrue(self.engine.should_handle(self.me, "BBS"))
        self.assertTrue(self.engine.should_handle(self.me, "bbs"))
        self.assertFalse(self.engine.should_handle(self.me, "hello"))

    def test_open_session_main_menu(self):
        r = self._h("BBS")
        self.assertTrue(r["handled"])
        self.assertTrue(r["session_active"])
        self.assertIn("Boards", r["reply"])
        self.assertIn("Mail", r["reply"])

    def test_ordinary_dm_not_handled_without_session(self):
        r = self._h("hey there")
        self.assertFalse(r["handled"])
        self.assertIsNone(r["reply"])

    def test_boards_list_and_post(self):
        self._h("BBS")
        r = self._h("B")
        self.assertIn("General", r["reply"])
        r = self._h("1")
        self.assertIn("List posts", r["reply"])
        r = self._h("N Field Day")
        self.assertIn("SEND", r["reply"])
        r = self._h("Who is going this weekend?")
        self.assertTrue(r["session_active"])
        r = self._h("SEND")
        self.assertIn("Posted", r["reply"])
        posts = self.store.list_posts(1)
        self.assertEqual(len(posts), 1)
        self.assertEqual(posts[0]["subject"], "Field Day")
        self.assertIn("weekend", posts[0]["body"])

    def test_read_post(self):
        self.store.add_post(1, self.other, "Hello", "Body text here")
        self._h("BBS")
        self._h("B")
        self._h("1")
        r = self._h("L")
        self.assertIn("Hello", r["reply"])
        r = self._h("1")
        self.assertIn("Body text", r["reply"])

    def test_author_delete(self):
        pid = self.store.add_post(1, self.me, "Mine", "secret")
        self._h("BBS")
        self._h("B")
        self._h("1")
        self._h("L")
        self._h("1")
        r = self._h("D")
        self.assertIn("Deleted", r["reply"])
        p = self.store.get_post(pid)
        self.assertEqual(p["status"], "deleted")

    def test_non_author_cannot_delete(self):
        self.store.add_post(1, self.other, "Theirs", "nope")
        self._h("BBS")
        self._h("B")
        self._h("1")
        self._h("L")
        self._h("1")
        r = self._h("D")
        self.assertIn("Only author", r["reply"])

    def test_mail_write_and_read(self):
        self._h("BBS")
        self._h("M")
        r = self._h("W ALICE Coffee?")
        self.assertIn("Composing", r["reply"])
        self._h("Bring thermos.")
        r = self._h("SEND")
        self.assertIn("Mail sent", r["reply"])
        # Recipient session
        r = self.engine.handle(self.other, "BBS")
        self.assertIn("1 new", r["reply"])
        r = self.engine.handle(self.other, "M")
        r = self.engine.handle(self.other, "1")
        self.assertIn("Coffee", r["reply"])
        self.assertIn("thermos", r["reply"])

    def test_unknown_recipient(self):
        self._h("BBS")
        self._h("M")
        r = self._h("W NOBODY Hi there")
        self.assertIn("Unknown node", r["reply"])

    def test_quit(self):
        self._h("BBS")
        r = self._h("X")
        self.assertFalse(r["session_active"])
        self.assertFalse(self.engine.should_handle(self.me, "1"))

    def test_cancel_compose(self):
        self._h("BBS")
        self._h("B")
        self._h("1")
        self._h("N Abort")
        r = self._h("CANCEL")
        self.assertIn("Cancelled", r["reply"])
        self.assertEqual(len(self.store.list_posts(1)), 0)

    def test_seed_general_board(self):
        boards = self.store.list_boards()
        self.assertEqual(len(boards), 1)
        self.assertEqual(boards[0]["name"], "General")


if __name__ == "__main__":
    unittest.main()
