"""
Collection of standard types used through the application.

These types can be aliases for intent clarify or wrappers
around common types.

For more application specific types, see models.
"""
from datetime import datetime, timedelta, timezone
import random
from typing import NewType, TypeAlias, Self
from uuid import UUID

from attrs import define, field


# region @types.std
# ---
# role: datatype
# intent: alias standard types for typechecking clarity.
# description:
# rules:
#   - Not used for application specific models. See models.
# dependencies:
# aliases:
# triggers:
# ---

SchemaVersion = NewType("SchemaVersion", str)
JsonAtom: TypeAlias = None | bool | int | float | str
JsonData: TypeAlias =  JsonAtom | dict[str, "JsonData"] | list["JsonData"]

# ---
# endregion

# region @types.time
# ---
# role: datatype
# intent: define time related types.
# description:
#   - Timestamp: Immutable timestamp wrapper with UTC timezone
#   - Period: time range with start and end Timestamps
#   - PeriodUUID: time range in UUIDv7 start and end format.
#   - Conversions between Timestamp and UUIDv7, and Period and PeriodUUID.
# rules:
#   - Timestamps MUST be timezone aware and MUST be in UTC.
# dependencies:
# aliases:
# triggers:
# ---

@define(slots=True)
class Timestamp:
    """
    Immutable timestamp wrapper with timezone awareness.

    All timestamps are stored in UTC for consistency.
    """
    value: datetime = field()

    @value.validator
    def check(self, _, value):
        if not isinstance(value, datetime):
            raise TypeError("Must be a datetime")
        if value.tzinfo is not UTC:
            raise ValueError("datetime must have tzinfo=UTC")

    @classmethod
    def now(cls) -> Self:
        """Get current UTC timestamp."""
        return cls(datetime.now(timezone.utc))

    @classmethod
    def from_datetime(cls, dt: datetime) -> Self:
        """Create from datetime, converting to UTC if needed."""
        if dt.tzinfo is None:
            # Assume UTC if no timezone specified
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            # Convert to UTC
            dt = dt.astimezone(timezone.utc)
        return cls(dt)

    @classmethod
    def from_iso(cls, iso_string: str) -> Self:
        """Parse from ISO 8601 format string."""
        dt = datetime.fromisoformat(iso_string)
        return cls.from_datetime(dt)

    def to_iso(self) -> str:
        """Convert to ISO 8601 format string."""
        return self.value.isoformat()

    @classmethod
    def from_uuid7(cls, u: UUID) -> Self:
        if u.version != 7:
            raise ValueError("Not a UUIDv7")
        # Top 48 bits = Unix timestamp in milliseconds
        return datetime.fromtimestamp(u.time / 1000, tz=timezone.utc)

    def to_uuid7(self) -> UUID:
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

    def __str__(self) -> str:
        """String representation in ISO format."""
        return self.to_iso()

    def __repr__(self) -> str:
        """Developer-friendly representation."""
        return f"Timestamp({self.to_iso()})"


@define
class Period:
    start: Timestamp | None = field(default=None)
    end: Timestamp | None = field(default=None)

    def duration(self) -> timedelta | None:
        if self.start is None or self.end is None:
            return None
        return self.end.value - self.start.value

    def to_uuid7(self) -> PeriodUUID:
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
        return f"{start}--{end}"

    def to_timestamp(self) -> Period:
        return Period(
            start=Timestamp.from_uuid7(self.start) if self.start is not None else None,
            end=Timestamp.from_uuid7(self.end) if self.end is not None else None,
        )

    def covers(self, uid: UUID) -> bool:
        return (
            (self.start is None or uid > self.start)
            and (self.end is None or uid < self.end)
        )

# ---
# endregion
