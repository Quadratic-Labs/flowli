"""Several workers on one bucket: in one process, across processes, and under fencing."""

import asyncio
import contextlib
import os
import signal
import subprocess
import sys
import time
from collections import Counter
from datetime import timedelta
from pathlib import Path

from flowli.adapters.cairndb import CairnBackend
from flowli.domain import Actor, EntryType, ExecutionStatus, Site, parse_eid
from flowli.runtime import Engine, EngineConfig

from . import concurrent_app

HUMAN = Actor.human("thomas@example.com")
ROOT = Path(__file__).resolve().parents[2]


def make_engine(bucket: str, worker_id: str, **config) -> tuple[Engine, CairnBackend]:
    """A fresh backend per engine: separate CairnDB instances, as separate processes have."""
    backend = CairnBackend.configure({"storage": {"type": "filesystem", "path": bucket}})
    engine = Engine(
        backend.ports,
        Site(host="h", pid=os.getpid(), worker_id=worker_id),
        registry=concurrent_app.registry,
        config=EngineConfig(poll_interval=0.02, **config),
    )
    return engine, backend


async def wait_until(predicate, timeout: float, interval: float = 0.05) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if await predicate():
            return
        await asyncio.sleep(interval)
    raise AssertionError("condition not met in time")


async def _queue_is_empty(backend, queue: str) -> bool:
    return await backend.queue.pending(queue) == []


async def all_terminal(engine: Engine, eids: list[str]) -> bool:
    for eid in eids:
        if not (await engine.status(eid)).is_terminal:
            return False
    return True


def by_type(entries) -> Counter:
    return Counter(s.item.type for s in entries)


# --- 1. in-process workers -----------------------------------------------------------------


async def test_four_workers_share_twelve_pipelines(tmp_path):
    bucket = str(tmp_path / "bucket")
    control, control_backend = make_engine(bucket, "control")
    workers = [make_engine(bucket, f"w-{i}") for i in range(4)]
    stop = asyncio.Event()
    try:
        eids = [await control.start(concurrent_app.pipeline, x, by=HUMAN) for x in range(12)]
        sweeper = control.sweeper()

        async def sweep_loop():
            while not stop.is_set():
                await sweeper.run_once()
                await asyncio.sleep(0.05)

        async def approver():
            """A human who approves whatever waits on its approve channel, once."""
            approved: set[str] = set()
            while not stop.is_set():
                for eid in eids:
                    if eid in approved:
                        continue
                    entries = await control.journal(eid)
                    waiting = [
                        s
                        for s in entries
                        if s.item.type == EntryType.FRAME_SUSPENDED
                        and s.item.payload["on"] == f"channel:{eid}.approve"
                    ]
                    if waiting:
                        await control.signal(eid, "approve", True, by=Actor.human("cfo@x"))
                        approved.add(eid)
                await asyncio.sleep(0.05)

        runners = [asyncio.create_task(e.worker().run_forever(stop)) for e, _ in workers]
        helpers = [asyncio.create_task(sweep_loop()), asyncio.create_task(approver())]
        await wait_until(lambda: all_terminal(control, eids), timeout=40)
        stop.set()
        await asyncio.gather(*runners, *helpers)

        seen_workers: Counter = Counter()
        for x, eid in enumerate(eids):
            assert await control.status(eid) == ExecutionStatus.COMPLETED
            entries = await control.journal(eid)
            counts = by_type(entries)
            assert counts[EntryType.EXECUTION_COMPLETED] == 1
            # each frame ran exactly once: no lease expired under these TTLs
            started = Counter(s.item.fid for s in entries if s.item.type == "frame.started")
            assert all(n == 1 for n in started.values()), started
            completed = entries[-1].item.payload["value"]
            assert completed == {"a": f"a:{x}", "b": f"b:{x}", "sq": x * x, "approved_by": "cfo@x"}
            for s in entries:
                seen_workers[s.item.provenance.site.worker_id] += 1
            epochs = [
                s.item.payload["epoch"] for s in entries if s.item.type == "execution.resumed"
            ]
            assert epochs == sorted(epochs) and len(set(epochs)) == len(epochs)
        assert len([w for w in seen_workers if w.startswith("w-")]) >= 2, seen_workers

        # the bucket is clean: no task, no wait marker, no timer
        assert await control_backend.queue.pending("default") == []
        assert await control_backend.channel.all_waits() == []
        assert await control_backend.timers.due(control.clock() + timedelta(days=365 * 70)) == []
        # 12 children completed too
        children = [
            parse_eid(s.item.payload["eid"])
            for s in await control_backend.control.read()
            if s.item.type == "execution.created" and s.item.payload.get("parent")
        ]
        assert len(children) == 12
        for child in children:
            assert await control.status(child) == ExecutionStatus.COMPLETED
    finally:
        stop.set()
        for _, b in workers:
            await b.close()
        await control_backend.close()


# --- 2. worker processes through the CLI ---------------------------------------------------


def spawn(*args: str, bucket: str) -> subprocess.Popen:
    env = {**os.environ, "PYTHONPATH": str(ROOT), "PYTHONUNBUFFERED": "1"}
    return subprocess.Popen(
        [
            sys.executable,
            "-m",
            "flowli.cli",
            *args,
            "--app",
            "tests.integration.concurrent_app:registry",
            "--storage-path",
            bucket,
            "--log-level",
            "WARNING",
        ],
        cwd=ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


async def test_worker_processes_share_one_bucket(tmp_path):
    bucket = str(tmp_path / "bucket")
    control, backend = make_engine(bucket, "control")
    procs: list[subprocess.Popen] = []
    try:
        eids = [await control.start(concurrent_app.simple, x, by=HUMAN) for x in range(8)]
        procs = [
            spawn("worker", "--worker-id", "p-1", "--poll-interval", "0.05", bucket=bucket),
            spawn("worker", "--worker-id", "p-2", "--poll-interval", "0.05", bucket=bucket),
            spawn("sweeper", "--interval", "0.2", bucket=bucket),
        ]
        await wait_until(lambda: all_terminal(control, eids), timeout=60)

        workers_seen: Counter = Counter()
        for x, eid in enumerate(eids):
            entries = await control.journal(eid)
            assert entries[-1].item.type == EntryType.EXECUTION_COMPLETED
            assert entries[-1].item.payload["value"] == [f"a:{x}", f"b:{x}", x * x]
            started = Counter(s.item.fid for s in entries if s.item.type == "frame.started")
            assert all(n == 1 for n in started.values()), started
            for s in entries:
                workers_seen[s.item.provenance.site.worker_id] += 1
        assert workers_seen["p-1"] > 0 and workers_seen["p-2"] > 0, workers_seen
        for eid in eids:
            for s in await control.journal(eid):
                assert s.item.provenance.site.pid != os.getpid()
    finally:
        for p in procs:
            p.send_signal(signal.SIGTERM)
        for p in procs:
            try:
                out, err = p.communicate(timeout=15)
            except subprocess.TimeoutExpired:
                p.kill()
                out, err = p.communicate()
            assert p.returncode == 0, f"exit {p.returncode}\n{out}\n{err}"
        await backend.close()


# --- 3. fencing: a paused worker is stolen from and recovered ----------------------------------


async def test_dead_worker_is_fenced_and_execution_recovered(tmp_path):
    bucket = str(tmp_path / "bucket")
    control, control_backend = make_engine(bucket, "control")
    slow, slow_backend = make_engine(bucket, "slow", exec_ttl=0.6, task_ttl=0.6)
    fresh, fresh_backend = make_engine(bucket, "fresh", exec_ttl=5.0, task_ttl=5.0)
    try:
        eid = await control.start(concurrent_app.slow_step, 2.0, by=HUMAN)
        # the slow worker takes the task; its renew loop runs every 0.2s and would keep the
        # lease alive, so freeze its renewals: this is a worker that stopped heartbeating.
        slow_worker = slow.worker()
        original_acquire = slow_backend.ownership.acquire

        async def acquire_without_renew(*a, **k):
            lease = await original_acquire(*a, **k)

            async def frozen_renew():
                await asyncio.sleep(3600)  # never actually renews

            lease.renew = frozen_renew
            return lease

        slow_backend.ownership.acquire = acquire_without_renew
        slow_task = asyncio.create_task(slow_worker.run_once())

        await asyncio.sleep(1.0)  # past the 0.6s lease TTL
        assert await control.status(eid) == ExecutionStatus.RUNNING
        report = await control.sweeper().run_once()
        assert report.recovered == [eid], report

        fresh_worker = fresh.worker()
        stop = asyncio.Event()
        runner = asyncio.create_task(fresh_worker.run_forever(stop))
        await wait_until(lambda: all_terminal(control, [eid]), timeout=20)
        # The fenced worker stops without acking its task (05-protocols.md,
        # section 8), so its leftover START task is acked by whoever dequeues
        # it next. Let the fresh worker reach it before we look at the queue.
        await wait_until(lambda: _queue_is_empty(control_backend, "default"), timeout=20)
        stop.set()
        await runner
        with contextlib.suppress(Exception, asyncio.CancelledError):
            await asyncio.wait_for(slow_task, timeout=10)

        entries = await control.journal(eid)
        counts = by_type(entries)
        assert counts[EntryType.EXECUTION_COMPLETED] == 1
        resumed = [s.item for s in entries if s.item.type == EntryType.EXECUTION_RESUMED]
        assert resumed and resumed[0].payload["reason"] == "recovery"
        assert resumed[0].payload["epoch"] == 2
        assert entries[-1].item.provenance.site.worker_id == "fresh"
        assert entries[-1].item.provenance.site.epoch == 2
        # the slow worker ran the step too. Its frame result may land (first wins), but it
        # publishes no lifecycle entry after the fence: the guarded check stops it.
        slow_entries = [s.item for s in entries if s.item.provenance.site.worker_id == "slow"]
        assert {e.type for e in slow_entries} <= {
            "execution.started",
            "frame.started",
            "frame.completed",
        }
        assert all(
            not e.type.startswith("execution.") or e.type == "execution.started"
            for e in slow_entries
        )
        assert await control_backend.queue.pending("default") == []
    finally:
        for b in (slow_backend, fresh_backend, control_backend):
            await b.close()
