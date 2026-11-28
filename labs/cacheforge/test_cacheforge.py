import unittest
from concurrent.futures import ThreadPoolExecutor
from threading import Event
from time import monotonic, sleep
from cacheforge import Cache

class CacheTests(unittest.TestCase):
    def test_hit(self):
        c = Cache()
        self.assertIsNone(c.get_or_load('x', lambda: None))
        self.assertIsNone(c.get_or_load('x', lambda: 2))
        self.assertEqual(c.stats()['hits'], 1)

    def test_expiry(self):
        now = [0]
        c = Cache(ttl=2, clock=lambda: now[0])
        self.assertEqual(c.get_or_load('x', lambda: 1), 1)
        now[0] = 2
        self.assertEqual(c.get_or_load('x', lambda: 2), 2)

    def test_lru(self):
        c = Cache(capacity=2)
        for k in 'aba' + 'c':
            c.get_or_load(k, lambda: k)
        self.assertEqual(c.get_or_load('b', lambda: 'new'), 'new')
        self.assertEqual(c.stats()['evictions'], 2)

    def test_failure_retry(self):
        c = Cache()
        def fail(): raise ValueError('failed')
        with self.assertRaises(ValueError): c.get_or_load('x', fail)
        self.assertEqual(c.get_or_load('x', lambda: 3), 3)

    def test_single_flight(self):
        c = Cache()
        entered, release = Event(), Event()
        def load():
            entered.set()
            if not release.wait(5): raise TimeoutError()
            return 42
        with ThreadPoolExecutor(max_workers=2) as pool:
            a = pool.submit(c.get_or_load, 'x', load)
            try:
                self.assertTrue(entered.wait(2))
                b = pool.submit(c.get_or_load, 'x', lambda: 99)
                deadline = monotonic() + 2
                while c.stats()['waits'] == 0 and monotonic() < deadline: sleep(.001)
                self.assertEqual(c.stats()['waits'], 1)
            finally: release.set()
            self.assertEqual((a.result(), b.result()), (42, 42))

    def test_recursive(self):
        c = Cache()
        with self.assertRaises(RuntimeError):
            c.get_or_load('x', lambda: c.get_or_load('x', lambda: 1))

    def test_invalid(self):
        with self.assertRaises(ValueError): Cache(capacity=0)

if __name__ == '__main__': unittest.main()
