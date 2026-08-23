"""Unit tests for BBS protocol helpers — no radio, no DB."""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bbs import protocol as proto


class ProtocolTests(unittest.TestCase):
    def test_utf8_budget_ascii(self):
        self.assertEqual(proto.utf8_len("hello"), 5)
        self.assertEqual(proto.fit("x" * 300, 20), "x" * 20)

    def test_fit_no_mid_codepoint(self):
        s = "ab" + "😀" + "cd"
        out = proto.fit(s, 5)
        out.encode("utf-8")  # must be valid
        self.assertLessEqual(proto.utf8_len(out), 5)

    def test_paginate_under_budget(self):
        items = [f"{i}) short" for i in range(1, 20)]
        pages = proto.paginate_lines("BOARDS", items, footer="0) Back", budget=220)
        self.assertTrue(pages)
        for p in pages:
            self.assertLessEqual(proto.utf8_len(p), 220)

    def test_parse_write_mail(self):
        cmd, to, subj = proto.split_write_mail("W KI5WKB Field Day plans")
        self.assertEqual(cmd, "W")
        self.assertEqual(to, "KI5WKB")
        self.assertEqual(subj, "Field Day plans")

    def test_parse_new_post(self):
        cmd, subj = proto.split_new_post("N Antenna notes")
        self.assertEqual(cmd, "N")
        self.assertEqual(subj, "Antenna notes")

    def test_bare_tokens(self):
        self.assertTrue(proto.is_bare_token("bbs", "BBS"))
        self.assertTrue(proto.is_bare_token("  SEND  ", "SEND"))
        self.assertFalse(proto.is_bare_token("SEND please", "SEND"))


if __name__ == "__main__":
    unittest.main()
