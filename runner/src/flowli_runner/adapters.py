"""Agent adapters. See specs/10-agent-runner.md section 6.

An adapter is the whole of what differs between agent runtimes. The handle it
returns is JSON-compatible and holds an address, never an object: it goes into
the task lease state, so a restarted runner or a thief can find the session
again.

Three families:

| Family | Example | `reattach` |
|---|---|---|
| subprocess | Claude Code, Codex, a shell command | a pid on this host |
| session | omnigent | a session id in a pool |
| service | OpenHands | a conversation over HTTP |
"""

from __future__ import annotations

import asyncio
import os
import signal
import socket
import subprocess
import time
from dataclasses import dataclass, field
from typing import Any, Protocol

from flowli.log import get_logger

from .envelope import Envelope

log = get_logger("flowli_runner.adapter")

Handle = dict[str, Any]


class AgentAdapter(Protocol):
    """Start, watch and stop one agent session."""

    async def start(self, envelope: Envelope) -> Handle: ...

    async def reattach(self, handle: Handle) -> Handle | None:
        """The same session, or None when it is gone. Never starts one."""
        ...

    async def poll(self, handle: Handle) -> dict[str, Any] | None:
        """The outcome when the session ended, None while it runs."""
        ...

    async def interrupt(self, handle: Handle, *, grace: float) -> dict[str, Any]:
        """Ask it to stop within `grace`, then stop it. Return the outcome."""
        ...

    async def terminate(self, handle: Handle) -> None:
        """Stop a session nobody will wait for. Never raises."""
        ...


# --- subprocess ---------------------------------------------------------------------


class _Running:
    """Not an exit code: the process is still there."""

    def __repr__(self) -> str:  # pragma: no cover - for a failure message
        return "<running>"


_RUNNING = _Running()


@dataclass
class SubprocessAgent:
    """Runs a command in the worktree, in its own process group.

    The command comes from `envelope.agent["command"]`, a list of argv strings
    in which `{intent}` is substituted. `omni claude -- …` is just a command,
    so a session runtime that spawns a harness uses this adapter too.
    """

    host: str = ""
    _exited: dict[int, int] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.host = self.host or socket.gethostname()

    async def start(self, envelope: Envelope) -> Handle:
        command = [str(a).format(intent=envelope.intent) for a in envelope.agent.get("command", [])]
        if not command:
            raise ValueError("envelope.agent.command is required for SubprocessAgent")
        cwd = envelope.worktree.path if envelope.worktree else None
        process = subprocess.Popen(  # noqa: S603 - the command is the envelope's
            command,
            cwd=cwd,
            start_new_session=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        log.info("agent_started", pid=process.pid, cwd=cwd)
        # The host is part of the handle: a runner elsewhere must not believe
        # it can reattach to a pid (section 6).
        return {
            "kind": "subprocess",
            "pid": process.pid,
            "host": self.host,
            "started_at": time.time(),
        }

    async def reattach(self, handle: Handle) -> Handle | None:
        if handle.get("host") != self.host:
            return None
        return handle if self._exit_code(int(handle["pid"])) is _RUNNING else None

    async def poll(self, handle: Handle) -> dict[str, Any] | None:
        code = self._exit_code(int(handle["pid"]))
        if code is _RUNNING:
            return None
        return {"outcome": "completed" if code in (0, None) else "failed", "exit_code": code}

    async def interrupt(self, handle: Handle, *, grace: float) -> dict[str, Any]:
        """The ladder: ask it to stop, wait out the grace, then kill it."""
        pid = int(handle["pid"])
        _signal_group(pid, signal.SIGTERM)
        deadline = time.monotonic() + grace
        while time.monotonic() < deadline:
            code = self._exit_code(pid)
            if code is not _RUNNING:
                return {"outcome": "interrupted", "exit_code": code}
            await asyncio.sleep(0.05)
        _signal_group(pid, signal.SIGKILL)
        self._exit_code(pid)  # reap it, so the process table stays clean
        return {"outcome": "interrupted", "killed": True}

    def _exit_code(self, pid: int) -> Any:
        """The exit code, None when it is not ours to reap, `_RUNNING` while it runs.

        A child that exited is a zombie until it is reaped, and `os.kill(pid, 0)`
        succeeds on a zombie — so a liveness check alone would wait forever.
        """
        if pid in self._exited:
            return self._exited[pid]
        try:
            reaped, status = os.waitpid(pid, os.WNOHANG)
        except ChildProcessError:
            # Not our child: this process did not start it (after a reattach in
            # a restarted runner, say). Fall back to a liveness check.
            return _RUNNING if _alive(pid) else None
        if reaped == 0:
            return _RUNNING
        code = os.waitstatus_to_exitcode(status)
        self._exited[pid] = code
        return code

    async def terminate(self, handle: Handle) -> None:
        if handle.get("host") != self.host:
            # Another host's process: we cannot stop it, and we must not
            # pretend we did.
            log.warning("foreign_handle_not_terminated", handle=handle)
            return
        _signal_group(int(handle["pid"]), signal.SIGKILL)


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _signal_group(pid: int, signum: int) -> None:
    try:
        os.killpg(os.getpgid(pid), signum)
    except ProcessLookupError, PermissionError:
        pass


# --- a hosted service ---------------------------------------------------------------


@dataclass
class ServiceAgent:
    """An agent runtime with its own session lifecycle, over HTTP.

    This is the OpenHands shape: a conversation is created, polled and stopped
    through an API, and it outlives this process. `reattach` is therefore a
    plain read, and it is the reason the handle must be serializable.

    The paths are configuration: `{id}` is the session id.
    """

    base_url: str
    token: str | None = None
    create_path: str = "/conversations"
    read_path: str = "/conversations/{id}"
    stop_path: str = "/conversations/{id}/stop"
    done_states: tuple[str, ...] = ("finished", "completed", "stopped", "error")
    client: Any = None

    def _http(self) -> Any:
        if self.client is None:
            import httpx

            headers = {"Authorization": f"Bearer {self.token}"} if self.token else {}
            self.client = httpx.AsyncClient(base_url=self.base_url, headers=headers, timeout=30.0)
        return self.client

    async def start(self, envelope: Envelope) -> Handle:
        body = {
            "intent": envelope.intent,
            "workdir": envelope.worktree.path if envelope.worktree else None,
            **envelope.agent,
        }
        response = await self._http().post(self.create_path, json=body)
        response.raise_for_status()
        data = response.json()
        session = str(data.get("id") or data.get("conversation_id"))
        log.info("agent_session_started", session=session)
        return {"kind": "service", "id": session, "url": self.base_url}

    async def reattach(self, handle: Handle) -> Handle | None:
        response = await self._http().get(self.read_path.format(id=handle["id"]))
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return handle

    async def poll(self, handle: Handle) -> dict[str, Any] | None:
        response = await self._http().get(self.read_path.format(id=handle["id"]))
        if response.status_code == 404:
            return {"outcome": "failed", "error": "the session is gone"}
        response.raise_for_status()
        data = response.json()
        state = str(data.get("status") or data.get("state") or "")
        if state not in self.done_states:
            return None
        return {
            "outcome": "completed" if state != "error" else "failed",
            "state": state,
            "result": data.get("result"),
        }

    async def interrupt(self, handle: Handle, *, grace: float) -> dict[str, Any]:
        del grace  # the service owns its own grace
        await self._http().post(self.stop_path.format(id=handle["id"]))
        return {"outcome": "interrupted", "state": "stopped"}

    async def terminate(self, handle: Handle) -> None:
        try:
            await self._http().post(self.stop_path.format(id=handle["id"]))
        except Exception:
            log.warning("session_not_terminated", session=handle.get("id"))

    async def aclose(self) -> None:
        if self.client is not None:
            await self.client.aclose()


__all__ = ["AgentAdapter", "Handle", "ServiceAgent", "SubprocessAgent"]
