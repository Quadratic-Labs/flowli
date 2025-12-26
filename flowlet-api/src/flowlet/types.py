from enum import StrEnum


class FlowType(StrEnum):
    flow = "flow"
    task = "task"


class RunStatus(StrEnum):
    running = "running"
    failed = "failed"
    success = "success"
    warning = "warning"

    @classmethod
    def from_log_level(cls, level: str) -> "RunStatus":
        """Map a logging level to a RunStatus.

        Args:
            level: The logging level name (e.g., 'INFO', 'SUCCESS', 'WARNING', 'ERROR')

        Returns:
            The corresponding RunStatus value
        """
        level_upper = level.upper()
        if level_upper == 'SUCCESS':
            return cls.success
        elif level_upper == 'WARNING':
            return cls.warning
        elif level_upper in ('ERROR', 'CRITICAL'):
            return cls.failed
        else:
            return cls.running