from datetime import datetime, timedelta, timezone
from enum import StrEnum
import random
from uuid import UUID

from attrs import define, field


class SpanType(StrEnum):
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


def datetime_from_uuid7(u: UUID) -> datetime:
    if u.version != 7:
        raise ValueError("Not a UUIDv7")
    # Top 48 bits = Unix timestamp in milliseconds
    return datetime.fromtimestamp(u.time / 1000, tz=timezone.utc)


def uuid7_from_datetime(dt: datetime) -> UUID:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    ts_ms = int(dt.timestamp() * 1000)
    # 48-bit timestamp
    uuid_int = ts_ms << 80
    # Add version (7)
    uuid_int |= 0x7 << 76
    # Add variant (RFC 4122)
    uuid_int |= 0b10 << 62
    # Fill remaining 74 bits with randomness
    uuid_int |= random.getrandbits(62)
    return UUID(int=uuid_int)



@define
class Period:
    start: datetime | None = field(default=None)
    end: datetime | None = field(default=None)

    def duration(self) -> timedelta | None:
        if self.start is None or self.end is None:
            return None
        return self.end - self.start

    def to_uuid7(self) -> PeriodUUID:
        start = self.start
        end = self.end
        if start is not None:
            start = uuid7_from_datetime(start)
        if end is not None:
            end = uuid7_from_datetime(end)
        return PeriodUUID(start, end)


@define
class PeriodUUID:
    start: UUID | None = field(default=None)
    end: UUID | None = field(default=None)

    def to_datetime(self) -> Period:
        start = self.start
        end = self.end
        if start is not None:
            start = datetime_from_uuid7(start)
        if end is not None:
            end = datetime_from_uuid7(end)
        return Period(start, end)

    def covers(self, uid: UUID) -> bool:
        return (
            (self.start is None or uid > self.start)
            and (self.end is None or uid < self.end)
        )