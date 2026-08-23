import os
import sqlite3
import tempfile
import time
import unittest

from store import MessageStore


class MessageStoreTests(unittest.TestCase):
    def make_db(self):
        fd, path = tempfile.mkstemp(suffix='.db')
        os.close(fd)
        return path

    def test_init_expires_stale_sending_messages(self):
        path = self.make_db()
        try:
            cutoff = time.time() - 120
            with sqlite3.connect(path) as conn:
                conn.executescript(
                    """
                    CREATE TABLE messages (
                        pkt_id INTEGER,
                        ts REAL NOT NULL,
                        from_id TEXT,
                        to_id TEXT,
                        direct INTEGER NOT NULL DEFAULT 0,
                        channel INTEGER NOT NULL DEFAULT 0,
                        via_mqtt INTEGER NOT NULL DEFAULT 0,
                        text TEXT NOT NULL,
                        status TEXT,
                        status_reason TEXT,
                        heard_by TEXT,
                        reactions TEXT,
                        PRIMARY KEY (pkt_id, ts)
                    );
                    """
                )
                conn.execute(
                    "INSERT INTO messages (pkt_id, ts, text, status) VALUES (?,?,?,?)",
                    (1, cutoff, 'stale', 'sending'),
                )
                conn.execute(
                    "INSERT INTO messages (pkt_id, ts, text, status) VALUES (?,?,?,?)",
                    (2, time.time(), 'fresh', 'sending'),
                )

            MessageStore(path=path)

            with sqlite3.connect(path) as conn:
                rows = conn.execute(
                    "SELECT pkt_id, status FROM messages ORDER BY pkt_id"
                ).fetchall()
            self.assertEqual(rows, [(1, 'no_ack'), (2, 'sending')])
        finally:
            os.unlink(path)

    def test_recent_since_returns_new_messages_and_older_updates(self):
        path = self.make_db()
        try:
            store = MessageStore(path=path)
            store.add({
                'id': 10,
                'ts': 100.0,
                'from': 'me',
                'to': '^all',
                'direct': False,
                'channel': 0,
                'via_mqtt': False,
                'text': 'older',
                'status': 'sending',
                'heard_by': [],
                'reactions': [],
            })
            store.add({
                'id': 11,
                'ts': 200.0,
                'from': 'me',
                'to': '^all',
                'direct': False,
                'channel': 0,
                'via_mqtt': False,
                'text': 'newer',
                'status': 'sending',
                'heard_by': [],
                'reactions': [],
            })
            store.update_status(10, 'no_ack', heard_by=[{'id': '!abc', 'via_mqtt': False}])

            delta = store.recent_since(150.0)

            self.assertEqual([m['id'] for m in delta['new']], [11])
            self.assertEqual([m['id'] for m in delta['updates']], [10])
            self.assertEqual(delta['updates'][0]['status'], 'no_ack')
            self.assertEqual(delta['updates'][0]['heard_by'][0]['id'], '!abc')
        finally:
            os.unlink(path)


if __name__ == '__main__':
    unittest.main()
