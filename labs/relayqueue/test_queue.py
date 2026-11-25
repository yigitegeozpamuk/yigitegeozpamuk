import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from queue_engine import Queue

class QueueTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.now = 100
        self.q = Queue(str(Path(self.tmp.name)/"jobs.db"), lambda:self.now)
    def test_idempotency(self):
        self.assertEqual(self.q.enqueue("a","x"),self.q.enqueue("a","x"))
        with self.assertRaises(ValueError):
            self.q.enqueue("a","different")
    def test_concurrent_claim(self):
        self.q.enqueue("a","x")
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(lambda _:self.q.claim(), range(8)))
        self.assertEqual(sum(r is not None for r in results),1)
    def test_expiry_and_fencing(self):
        jid = self.q.enqueue("a","x")
        old = self.q.claim(5)
        self.now += 5
        new = self.q.claim()
        self.assertFalse(self.q.finish(jid,old["token"]))
        self.assertTrue(self.q.finish(jid,new["token"]))
        self.assertEqual(self.q.status(),{"done":1})
    def test_retry_dead_letter(self):
        jid = self.q.enqueue("a","x",2)
        first = self.q.claim()
        self.assertTrue(self.q.finish(jid,first["token"],False))
        self.assertIsNone(self.q.claim())
        self.now += 2
        second = self.q.claim()
        self.q.finish(jid,second["token"],False)
        self.assertEqual(self.q.status(),{"dead":1})
    def test_expired_final_attempt(self):
        self.q.enqueue("a","x",1)
        self.q.claim(1)
        self.now += 1
        self.assertIsNone(self.q.claim())
        self.assertEqual(self.q.status(),{"dead":1})
    def test_restart(self):
        jid = self.q.enqueue("a","x")
        reopened = Queue(self.q.path,lambda:self.now)
        self.assertEqual(reopened.claim()["id"],jid)

if __name__ == "__main__":
    unittest.main()
