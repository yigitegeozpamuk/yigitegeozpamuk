"""Bounded, thread-safe LRU cache with TTL and single-flight loading."""
from collections import OrderedDict
from concurrent.futures import Future
from threading import RLock, get_ident
from time import monotonic


class Cache:
    def __init__(self, capacity=128, ttl=60, clock=monotonic):
        if capacity < 1 or ttl <= 0:
            raise ValueError('capacity and ttl must be positive')
        self.capacity, self.ttl, self.clock = capacity, ttl, clock
        self._data = OrderedDict()
        self._pending = {}
        self._lock = RLock()
        self._stats = dict(hits=0, misses=0, evictions=0, waits=0)

    def get_or_load(self, key, loader):
        with self._lock:
            if key in self._data:
                value, expiry = self._data[key]
                if self.clock() < expiry:
                    self._data.move_to_end(key)
                    self._stats['hits'] += 1
                    return value
                del self._data[key]
            self._stats['misses'] += 1
            if key in self._pending:
                future, owner = self._pending[key]
                if owner == get_ident():
                    raise RuntimeError('recursive load for same key')
                self._stats['waits'] += 1
                leader = False
            else:
                future = Future()
                self._pending[key] = (future, get_ident())
                leader = True
        if not leader:
            return future.result()
        try:
            value = loader()
            with self._lock:
                now = self.clock()
                for expired in [k for k, (_, e) in self._data.items() if e <= now]:
                    del self._data[expired]
                self._data[key] = (value, now + self.ttl)
                if len(self._data) > self.capacity:
                    self._data.popitem(last=False)
                    self._stats['evictions'] += 1
                future.set_result(value)
                del self._pending[key]
            return value
        except BaseException as exc:
            with self._lock:
                future.set_exception(exc)
                del self._pending[key]
            raise

    def stats(self):
        with self._lock:
            return dict(self._stats)


if __name__ == '__main__':
    cache = Cache()
    print(cache.get_or_load('demo', lambda: 42))
    print(cache.get_or_load('demo', lambda: 99))
    print(cache.stats())
