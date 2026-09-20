"""Domain errors. See docs/specs/04-api.md section 4."""

from __future__ import annotations

from typing import Any


class FlowletError(Exception):
    """Base of all domain errors."""


class InvalidName(FlowletError, ValueError):
    """An identifier does not match its required pattern."""


class NondeterminismError(FlowletError):
    """Replay found a frame id with a different args digest."""

    def __init__(self, fid: str, recorded: str, computed: str) -> None:
        super().__init__(f"frame {fid}: args digest {computed} differs from journal {recorded}")
        self.fid = fid
        self.recorded = recorded
        self.computed = computed


class DuplicateFrameError(FlowletError):
    """A frame key repeats under one parent."""

    def __init__(self, fid: str) -> None:
        super().__init__(f"duplicate frame id {fid}")
        self.fid = fid


class ChildFailed(FlowletError):
    """A child execution failed or was cancelled."""

    def __init__(self, child_eid: Any, status: str, error: str | None = None) -> None:
        super().__init__(f"child {child_eid} {status}: {error or ''}".rstrip())
        self.child_eid = child_eid
        self.status = status
        self.error = error


class LeaseLost(FlowletError):
    """The execution lease was stolen. Stop all work on the execution."""


class Cancelled(FlowletError):
    """Raised inside the workflow function at a frame boundary after a cancel request."""


class WorkflowNotRegistered(FlowletError):
    def __init__(self, workflow: str, version: str) -> None:
        super().__init__(f"workflow {workflow!r} version {version!r} is not registered")
        self.workflow = workflow
        self.version = version


class NonRetryableError(Exception):
    """A step raises this, or a subclass, to stop retries. The frame fails at once."""


class StepFailed(FlowletError):
    """A step frame has no attempt left. Raised in the workflow function on every replay."""

    def __init__(self, fid: str, failed: Any) -> None:
        super().__init__(f"step {fid} failed: {failed.error_type}: {failed.message}")
        self.fid = fid
        self.failed = failed
