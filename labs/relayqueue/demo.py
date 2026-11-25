from pathlib import Path
from tempfile import TemporaryDirectory
from queue_engine import Queue

with TemporaryDirectory() as d:
    q = Queue(str(Path(d)/"jobs.db"))
    q.enqueue("order-42","generate invoice")
    job = q.claim()
    print("Processing:",job["payload"])
    q.finish(job["id"],job["token"])
    print("Queue state:",q.status())
