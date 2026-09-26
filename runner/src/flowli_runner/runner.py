"""The agent runner: the `Handler` of a delegate task that runs a coding agent.

See specs/10-agent-runner.md sections 3 to 9. The loop, the heartbeat, the
recovery and the cancel are `flowli.runtime.Consumer`; this is the work it
drives.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import Any

from flowli.domain import EvidenceRef
from flowli.log import get_logger
from flowli.runtime import Consumer, ConsumerConfig, Held, Refused

from . import worktree as wt
from .adapters import AgentAdapter, Handle
from .envelope import Envelope, Report, Worktree
from .scopes import ScopeSet, acquire

log = get_logger("flowli_runner")


@dataclass(frozen=True, slots=True)
class RunnerConfig:
    """See specs/10-agent-runner.md section 11."""

    repo_path: str
    base_ref: str = "HEAD"
    scope_ttl: float = 900.0
    quarantine_on_interrupt: bool = True
    keep_worktree: bool = False  # a person may want to look at the tree


@dataclass
class _Attempt:
    """What this runner acquired for one task, and must give back."""

    envelope: Envelope
    worktree: Worktree | None = None
    scopes: ScopeSet | None = None
    interrupted: bool = False
    evidence: list[str] = field(default_factory=list)


class AgentRunner:
    """Prepares a substrate, runs an agent on it, and reports what came of it."""

    def __init__(
        self,
        engine: Any,
        adapter: AgentAdapter,
        config: RunnerConfig,
        *,
        db: Any = None,
    ) -> None:
        self.engine = engine
        self.adapter = adapter
        self.config = config
        # The scopes need CairnDB itself: they are leases on names of their own,
        # not on anything the engine owns.
        self._db = db
        self._attempts: dict[str, _Attempt] = {}
        # A command the deployment configures, for an envelope that names none.
        self.default_command: str | None = None

    # --- Handler -----------------------------------------------------------------

    async def prepare(self, held: Held) -> None:
        envelope = Envelope.from_payload(held.task.payload)
        attempt = _Attempt(envelope)
        self._attempts[held.task_id] = attempt

        if envelope.write_scope and self._db is not None:
            scopes = await acquire(
                self._db,
                envelope.write_scope,
                holder=held.task_id,
                ttl=self.config.scope_ttl,
            )
            if scopes is None:
                # Another attempt writes those paths. The task goes back on the
                # queue: it is ready, not blocked (section 7).
                del self._attempts[held.task_id]
                raise Refused("write scopes are held elsewhere", timedelta(seconds=30))
            attempt.scopes = scopes

        path, branch, base = wt.create(
            self.config.repo_path,
            eid=str(held.task.eid),
            key=_attempt_key(held),
            base_ref=envelope.base_ref or self.config.base_ref,
        )
        attempt.worktree = Worktree(path=path, branch=branch, base_commit=base)

    async def start(self, held: Held) -> Handle:
        attempt = self._attempts[held.task_id]
        envelope = _with_worktree(attempt.envelope, attempt.worktree)
        if self.default_command and not envelope.agent.get("command"):
            from shlex import split

            envelope = _with_command(envelope, split(self.default_command))
        attempt.envelope = envelope
        return await self.adapter.start(envelope)

    async def reattach(self, held: Held, handle: Handle) -> Handle | None:
        del held
        return await self.adapter.reattach(handle)

    async def poll(self, held: Held, handle: Handle) -> dict[str, Any] | None:
        outcome = await self.adapter.poll(handle)
        if outcome is None:
            return None
        attempt = self._attempts[held.task_id]
        if attempt.scopes is not None:
            await attempt.scopes.renew()
        return (await self._report(held, outcome)).to_payload()

    async def interrupt(self, held: Held, handle: Handle, *, grace: float) -> dict[str, Any]:
        attempt = self._attempts[held.task_id]
        attempt.interrupted = True
        outcome = await self.adapter.interrupt(handle, grace=grace)
        # The runner answers even when it is cancelled: a cancelled execution
        # ignores the answer, and a suspended one gets a real outcome instead
        # of a timeout (section 5).
        return (await self._report(held, outcome)).to_payload()

    async def terminate(self, held: Held, handle: Handle) -> None:
        del held
        await self.adapter.terminate(handle)

    async def release(self, held: Held) -> None:
        attempt = self._attempts.pop(held.task_id, None)
        if attempt is None:
            return
        if attempt.scopes is not None:
            await attempt.scopes.release()
        tree = attempt.worktree
        if tree is None or self.config.keep_worktree:
            return
        if attempt.interrupted and self.config.quarantine_on_interrupt:
            name = f"{held.task.eid}-{_attempt_key(held)}"
            wt.quarantine(self.config.repo_path, tree.path, name=name)
        else:
            wt.remove(self.config.repo_path, tree.path)

    # --- the report ---------------------------------------------------------------

    async def _report(self, held: Held, outcome: dict[str, Any]) -> Report:
        attempt = self._attempts[held.task_id]
        tree = attempt.worktree
        commits = wt.new_commits(tree.path, tree.base_commit) if tree else []
        verification = await self._verify(attempt)
        evidence = await self._write_evidence(held, attempt, outcome, verification)
        return Report(
            outcome=str(outcome.get("outcome", "completed")),
            summary=str(outcome.get("summary", "")),
            commits=commits,
            branch=tree.branch if tree else None,
            base_commit=tree.base_commit if tree else None,
            verification=verification,
            evidence=evidence,
            error=outcome.get("error"),
            resume=outcome.get("resume"),
            cost=dict(outcome.get("cost") or {}),
        )

    async def _verify(self, attempt: _Attempt) -> list[dict[str, Any]]:
        """Run the envelope's commands. Their exit codes are evidence; the
        workflow applies the gates (section 8)."""
        import asyncio

        results: list[dict[str, Any]] = []
        tree = attempt.worktree
        for command in attempt.envelope.verification:
            if tree is None:
                break
            started = asyncio.get_running_loop().time()
            process = await asyncio.create_subprocess_shell(
                command,
                cwd=tree.path,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )
            out, _ = await process.communicate()
            results.append(
                {
                    "command": command,
                    "exit_code": process.returncode,
                    "duration_ms": int((asyncio.get_running_loop().time() - started) * 1000),
                    "output": out.decode(errors="replace")[-4000:],
                }
            )
        return results

    async def _write_evidence(
        self,
        held: Held,
        attempt: _Attempt,
        outcome: dict[str, Any],
        verification: list[dict[str, Any]],
    ) -> list[str]:
        """A transcript and a diff are too large for a reply payload, so they
        go to evidence and the report carries the references (section 9)."""
        ref = EvidenceRef(held.task.eid, held.task.waiting_fid, _attempt_number(held))
        written: list[str] = []
        port = self.engine.ports.evidence

        transcript = outcome.get("transcript")
        if isinstance(transcript, str) and transcript:
            written.append(await port.put(ref, "transcript.txt", transcript.encode(), "text/plain"))
        if verification:
            body = "\n\n".join(
                f"$ {v['command']}\nexit {v['exit_code']}\n{v['output']}" for v in verification
            )
            written.append(await port.put(ref, "verification.txt", body.encode(), "text/plain"))
        tree = attempt.worktree
        if tree is not None and Path(tree.path).exists():
            try:
                diff = wt.git(tree.path, "diff", f"{tree.base_commit}..HEAD")
            except Exception:
                diff = ""
            if diff:
                written.append(await port.put(ref, "diff.patch", diff.encode(), "text/x-patch"))
        return written


def _attempt_key(held: Held) -> str:
    """The attempt's name in the branch: the task's dedup key, which the
    workflow chose (a rework is a new key, so a new branch; a re-dequeue of
    one task is the same key, so the same branch)."""
    raw = held.claimed.task.key or held.task.fid
    return raw.replace("/", "-").replace("#", "-").replace(":", "-")


def _attempt_number(held: Held) -> int:
    """The epoch of the task lease: one number per real run of this task by
    one consumer, so two runs never overwrite each other's evidence."""
    return int(held.lease.epoch)


def _with_worktree(envelope: Envelope, tree: Worktree | None) -> Envelope:
    from dataclasses import replace

    return replace(envelope, worktree=tree)


def _with_command(envelope: Envelope, command: list[str]) -> Envelope:
    from dataclasses import replace

    return replace(envelope, agent={**envelope.agent, "command": command})


def build_consumer(
    engine: Any,
    adapter: AgentAdapter,
    config: RunnerConfig,
    consumer_config: ConsumerConfig,
    *,
    db: Any = None,
) -> Consumer:
    """A consumer that runs agents. The runner is its `Handler`."""
    return Consumer(engine, AgentRunner(engine, adapter, config, db=db), consumer_config)


__all__ = ["AgentRunner", "RunnerConfig", "build_consumer"]
