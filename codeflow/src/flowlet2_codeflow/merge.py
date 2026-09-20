"""The merge queue. See flowlet/docs/specs/11-codeflow.md section 7.

One writer to the integration branch, always. The writer is a `Consumer` of
the `merge` queue with the handler below: one consumer holds one task at a
time, so the queue lease is the single-writer discipline. No resident process,
and no lock of its own.

It never ends, so it is a process and not a workflow (section 2.1).
"""

from __future__ import annotations

import asyncio
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from flowlet.log import get_logger
from flowlet.runtime import Consumer, ConsumerConfig, Held
from flowlet_runner import worktree as wt

log = get_logger("flowlet_codeflow.merge")


@dataclass(frozen=True, slots=True)
class MergeConfig:
    repo_path: str
    integration_branch: str = "main"
    gates: tuple[str, ...] = ()  # run on the merged tree, before it is published
    post_merge: tuple[str, ...] = ()
    committer: str = "CodeFlow <codeflow@example.com>"


@dataclass
class MergeHandler:
    """Rebase, gate, merge, gate again. A failure is an answer, not a stop."""

    config: MergeConfig
    _trees: dict[str, str] = field(default_factory=dict)

    # --- Handler ------------------------------------------------------------

    async def prepare(self, held: Held) -> None:
        return None

    async def start(self, held: Held) -> dict[str, Any]:
        """The whole merge is one call: it is short, and nothing outlives it."""
        request = dict(held.task.payload or {})
        branch = str(request.get("branch") or "")
        if not branch:
            return {"handle": "done", "result": {"merged": False, "reason": "no branch"}}
        result = await asyncio.to_thread(self._merge, branch)
        return {"handle": "done", "result": result}

    async def reattach(self, held: Held, handle: dict[str, Any]) -> dict[str, Any] | None:
        # A merge is not resumable: it either published a commit or it did not,
        # and the integration branch says which. Start it again.
        return None

    async def poll(self, held: Held, handle: dict[str, Any]) -> dict[str, Any] | None:
        return dict(handle["result"])

    async def interrupt(
        self, held: Held, handle: dict[str, Any], *, grace: float
    ) -> dict[str, Any]:
        return {"merged": False, "reason": "the merge queue was asked to stop"}

    async def terminate(self, held: Held, handle: dict[str, Any]) -> None:
        return None

    async def release(self, held: Held) -> None:
        return None

    # --- the merge ----------------------------------------------------------

    def _merge(self, branch: str) -> dict[str, Any]:
        repo = self.config.repo_path
        integration = self.config.integration_branch
        path = str(Path(repo).resolve() / "mq" / branch.replace("/", "-"))
        wt.remove(repo, path)
        try:
            wt.git(repo, "worktree", "add", "--detach", path, integration)
        except subprocess.CalledProcessError as exc:
            return {"merged": False, "reason": f"the integration branch is unusable: {exc}"}

        try:
            try:
                wt.git(path, "-c", f"user.name={self.config.committer.split(' <')[0]}",
                       "-c", f"user.email={self.config.committer.split('<')[1].rstrip('>')}",
                       "merge", "--no-ff", "-m", f"merge {branch}", branch)
            except subprocess.CalledProcessError as exc:
                conflicts = _conflicts(path)
                wt.git(path, "merge", "--abort")
                # The queue is not blocked by one bad merge: the task goes
                # back to rework with this reason.
                return {
                    "merged": False,
                    "reason": "conflict" if conflicts else f"the merge failed: {exc}",
                    "conflicts": conflicts,
                }

            failed = _run_all(path, self.config.gates)
            if failed is not None:
                return {"merged": False, "reason": "a merge gate failed", "gate": failed}

            commit = wt.git(path, "rev-parse", "HEAD")
            wt.git(repo, "update-ref", f"refs/heads/{integration}", commit)

            failed = _run_all(path, self.config.post_merge)
            if failed is not None:
                log.warning("post_merge_gate_failed", branch=branch, gate=failed["command"])
                return {"merged": True, "commit": commit, "post_merge_failed": failed}

            log.info("branch_merged", branch=branch, commit=commit)
            return {"merged": True, "commit": commit}
        finally:
            wt.remove(repo, path)


def _conflicts(path: str) -> list[str]:
    try:
        out = wt.git(path, "diff", "--name-only", "--diff-filter=U")
    except subprocess.CalledProcessError:
        return []
    return [line for line in out.splitlines() if line]


def _run_all(path: str, commands: tuple[str, ...]) -> dict[str, Any] | None:
    for command in commands:
        result = subprocess.run(
            command, cwd=path, shell=True, capture_output=True, text=True
        )
        if result.returncode != 0:
            return {
                "command": command,
                "exit_code": result.returncode,
                "output": (result.stdout + result.stderr)[-4000:],
            }
    return None


def build_consumer(engine: Any, config: MergeConfig, *, holder: str = "merge-queue",
                   queue: str = "merge", ttl: float = 600.0) -> Consumer:
    """The merge queue as a process. One consumer, so one writer."""
    return Consumer(
        engine,
        MergeHandler(config),
        ConsumerConfig(queues=(queue,), holder=holder, ttl=ttl),
    )


__all__ = ["MergeConfig", "MergeHandler", "build_consumer"]
