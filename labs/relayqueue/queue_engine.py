"""Durable SQLite job queue with atomic leases and fencing tokens."""
import sqlite3
import time
import uuid
from contextlib import contextmanager

class Queue:
    def __init__(self, path, clock=time.time):
        self.path, self.clock = path, clock
        with self.tx() as db:
            db.execute("""CREATE TABLE IF NOT EXISTS jobs(
                id TEXT PRIMARY KEY, idem TEXT UNIQUE NOT NULL, payload TEXT NOT NULL,
                state TEXT NOT NULL DEFAULT 'ready', attempts INTEGER NOT NULL DEFAULT 0,
                max_attempts INTEGER NOT NULL, available REAL NOT NULL,
                lease_until REAL, token TEXT)""")
            db.execute("CREATE INDEX IF NOT EXISTS job_ready ON jobs(state, available)")
    @contextmanager
    def tx(self):
        db = sqlite3.connect(self.path, timeout=10, isolation_level=None)
        db.row_factory = sqlite3.Row
        try:
            db.execute("BEGIN IMMEDIATE")
            yield db
            db.commit()
        except BaseException:
            db.rollback()
            raise
        finally:
            db.close()
    def enqueue(self, key, payload, max_attempts=3):
        if not key or not isinstance(payload, str) or max_attempts < 1:
            raise ValueError("Invalid job")
        with self.tx() as db:
            prior = db.execute("SELECT * FROM jobs WHERE idem=?", (key,)).fetchone()
            if prior:
                if prior["payload"] != payload or prior["max_attempts"] != max_attempts:
                    raise ValueError("Idempotency conflict")
                return prior["id"]
            jid = uuid.uuid4().hex
            db.execute("INSERT INTO jobs(id,idem,payload,max_attempts,available) VALUES(?,?,?,?,?)",
                       (jid,key,payload,max_attempts,self.clock()))
            return jid
    def claim(self, lease_seconds=30):
        if lease_seconds <= 0:
            raise ValueError("Lease must be positive")
        with self.tx() as db:
            now = self.clock()
            db.execute("""UPDATE jobs SET state=CASE WHEN attempts>=max_attempts THEN 'dead'
                ELSE 'ready' END, token=NULL, lease_until=NULL, available=?
                WHERE state='leased' AND lease_until<=?""",(now,now))
            row = db.execute("""SELECT * FROM jobs WHERE state='ready' AND available<=?
                ORDER BY available,id LIMIT 1""",(now,)).fetchone()
            if not row:
                return None
            token = uuid.uuid4().hex
            db.execute("""UPDATE jobs SET state='leased', attempts=attempts+1,
                       token=?, lease_until=? WHERE id=?""",(token,now+lease_seconds,row["id"]))
            return dict(db.execute("SELECT * FROM jobs WHERE id=?",(row["id"],)).fetchone())
    def finish(self, jid, token, success=True):
        with self.tx() as db:
            now = self.clock()
            row = db.execute("""SELECT * FROM jobs WHERE id=? AND token=? AND
                state='leased' AND lease_until>?""",(jid,token,now)).fetchone()
            if not row:
                return False
            state = 'done' if success else ('dead' if row['attempts']>=row['max_attempts'] else 'ready')
            delay = min(300, 2**min(row['attempts'],9)) if state=='ready' else 0
            db.execute("""UPDATE jobs SET state=?,available=?,token=NULL,lease_until=NULL
                WHERE id=?""",(state,now+delay,jid))
            return True
    def status(self):
        with self.tx() as db:
            return {r[0]:r[1] for r in db.execute("SELECT state,count(*) FROM jobs GROUP BY state")}
