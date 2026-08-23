"""
Collection of standard types used through the application.

These types can be aliases for intent clarify or wrappers
around common types.

For more application specific types, see models.
"""
from collections.abc import Callable
from datetime import timedelta
from typing import NewType, Self, TypeAlias, TypeVar
from uuid import UUID

from attrs import define, field
from cairndb import Timestamp as Timestamp

SchemaVersion = NewType("SchemaVersion", str)
JsonAtom: TypeAlias = None | bool | int | float | str
JsonData: TypeAlias =  JsonAtom | dict[str, "JsonData"] | list["JsonData"]


T = TypeVar("T")
R = TypeVar("R")
def map_or_none(value: T | None, func: Callable[[T], R]) -> R | None:
    return func(value) if value is not None else None


@define
class Period:
    """Optional [start, end] wall-clock time range.

    Both bounds are inclusive-open (covers() uses strict inequalities).
    Either bound may be None to represent an open interval.
    Convert to PeriodUUID with to_uuid() for use as a storage query key.
    """

    start: Timestamp | None = field(default=None)
    end: Timestamp | None = field(default=None)

    def duration(self) -> timedelta | None:
        if self.start is None or self.end is None:
            return None
        return self.end.value - self.start.value

    def to_uuid(self) -> PeriodUUID:
        """Convert to a PeriodUUID using standard UUIDv7 encoding."""
        return PeriodUUID(
            start=self.start.to_uuid7() if self.start is not None else None,
            end=self.end.to_uuid7() if self.end is not None else None,
        )

    def covers(self, ts: Timestamp) -> bool:
        return (
            (self.start is None or ts.value > self.start.value)
            and (self.end is None or ts.value < self.end.value)
        )


@define
class PeriodUUID:
    """A time range encoded as a pair of standard UUIDv7 values.

    Used as a compact, lexicographically sortable query key in storage paths
    and database range scans.  UUIDv7 embeds the millisecond timestamp in its
    top 48 bits, so UUID ordering equals chronological ordering.

    Construct from a Period with Period.to_uuid(), or parse the
    ``<start>--<end>`` string representation with from_str().
    Convert back to wall-clock time with to_timestamp().
    """

    start: UUID | None = field(default=None)
    end: UUID | None = field(default=None)

    @classmethod
    def from_str(cls, value) -> Self:
        parts = value.split("--")
        if len(parts) != 2:
            raise ValueError(f"Invalid PeriodUUID {value}: MUST follow the format <start_uuid>--<end_uuid>")
        start = UUID(parts[0])
        end = UUID(parts[1])
        return cls(start=start, end=end)

    def __str__(self) -> str:
        return f"{self.start}--{self.end}"

    def to_timestamp(self) -> Period:
        """Convert to a Period by extracting timestamps from the UUIDv7 bounds."""
        return Period(
            start=Timestamp.from_uuid7(self.start) if self.start is not None else None,
            end=Timestamp.from_uuid7(self.end) if self.end is not None else None,
        )

    def covers(self, uid: UUID) -> bool:
        """Return True if uid falls within this period.

        UUIDv7 ordering equals chronological ordering, so this mirrors
        Period.covers(): strictly inside the (start, end) interval, with
        None meaning an open bound.
        """
        return (
            (self.start is None or uid > self.start)
            and (self.end is None or uid < self.end)
        )
