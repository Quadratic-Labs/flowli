from enum import StrEnum


class FlowType(StrEnum):
    flow = "flow"
    task = "task"


class RunStatus(StrEnum):
    running = "running"
    failed = "failed"
    success = "success"