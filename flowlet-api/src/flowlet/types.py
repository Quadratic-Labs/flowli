"""
Collection of standard types used through the application.

These types can be aliases for intent clarify or wrappers
around common types.

For more application specific types, see models.
"""
from datetime import datetime, timedelta, timezone, UTC
import random
import secrets
from typing import Callable, NewType, TypeAlias, Self, TypeVar
from uuid import UUID

from attrs import define, field


# region @types.std
# ---
# role: datatype
# intent: alias standard types for typechecking clarity.
# description: >
#   SchemaVersion: opaque string that carries a semantic-version tag for
#   serialized payloads, preventing silent schema mismatches.
#   JsonAtom: leaf-level JSON scalar (None | bool | int | float | str).
#   JsonData: fully recursive JSON value type used at serialisation boundaries.
# rules:
#   - Not used for application specific models. See models.
# dependencies:
# aliases:
#   - json
#   - schema version
# triggers:
#   - what is JsonData
#   - how is schema version typed
# ---

SchemaVersion = NewType("SchemaVersion", str)
JsonAtom: TypeAlias = None | bool | int | float | str
JsonData: TypeAlias =  JsonAtom | dict[str, "JsonData"] | list["JsonData"]

# ---
# endregion

# region @types.uuid
# ---
# role: datatype
# intent: generate time-ordered UUIDs with descending (newest-first) lexicographic sort
# description: >
#   uuid7_desc is a drop-in variant of UUIDv7 (RFC 9562): same 128-bit layout
#   (48-bit ms timestamp | version 0x7 | 12 rand_a | variant 0b10 | 62 rand_b)
#   but the timestamp bits are bitwise-inverted so lexicographic order is
#   newest-first. Use for run_id / span_id so blob and filesystem listings
#   show the most recent entries first without any extra sort step.
# rules:
#   - Generated UUIDs MUST sort newest-first in lexicographic (string) order.
#   - The version nibble MUST remain 0x7 for compatibility with UUIDv7 parsers.
#   - Randomness MUST use secrets.randbits (not random) for unpredictability.
# dependencies:
# aliases:
#   - uuid7_desc
#   - run id
#   - span id
# triggers:
#   - how are run IDs generated
#   - what is uuid7_desc
#   - why do IDs sort newest first
# ---

def uuid7_desc() -> UUID:
    """Time-ordered UUID with inverted timestamp for descending lexicographic sort.

    Identical bit layout to UUIDv7 (RFC 9562) except the 48-bit millisecond
    timestamp is bitwise-inverted, so lexicographic ordering is newest-first.
    Use this for run_id / span_id so blob and filesystem listings are newest-first.

    Layout (128 bits):
        127..80  ~unix_ts_ms  48-bit inverted timestamp
         79..76  0x7          version (same marker as UUIDv7)
         75..64  rand_a       12 random bits
         63..62  0b10         RFC 4122 variant
         61..0   rand_b       62 random bits

    Returns:
        A UUID whose string representation sorts newest-first.
    """
    ts_ms  = int(datetime.now(UTC).timestamp() * 1000)
    inv_ts = (~ts_ms) & 0xFFFF_FFFF_FFFF
    rand_a = secrets.randbits(12)
    rand_b = secrets.randbits(62)
    return UUID(int=(
          (inv_ts << 80)   # bits 127..80  inverted timestamp
        | (0x7    << 76)   # bits  79..76  version
        | (rand_a << 64)   # bits  75..64  rand_a
        | (0b10   << 62)   # bits  63..62  variant
        |  rand_b          # bits  61..0   rand_b
    ))

# ---
# endregion

# region @types.time
# ---
# role: datatype
# intent: UTC-aware time primitives and their uuid7_desc UUID representations
# description: >
#   Timestamp wraps datetime, enforces UTC, and is the single source of truth
#   for wall-clock time. Period is an optional [start, end] Timestamp range.
#   PeriodUUID is the uuid7_desc-encoded form of Period used as a query key in
#   storage paths and database range scans.
#   Primary conversion API — Timestamp.to_uuid() / Timestamp.from_uuid() — uses
#   uuid7_desc exclusively (newest-first lexicographic ordering).
#   PeriodUUID.covers() inverts the usual UUID comparison because a newer
#   timestamp maps to a smaller uuid7_desc value.
# rules:
#   - Timestamps MUST be timezone-aware and MUST be in UTC.
#   - to_uuid / from_uuid MUST use uuid7_desc (newest-first ordering).
#   - PeriodUUID MUST only be constructed from uuid7_desc UUIDs.
#   - Period.covers() compares datetime values; PeriodUUID.covers() compares UUID values.
# dependencies:
#   - types.uuid
# aliases:
#   - timestamp
#   - period
#   - period uuid
# triggers:
#   - how to convert a timestamp to a UUID
#   - what is PeriodUUID
#   - how does covers work
#   - how to filter runs by time range
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
        """Extract timestamp from a standard UUIDv7.

        Args:
            u: A standard UUIDv7.

        Returns:
            The Timestamp embedded in the UUID.
        """
        if u.version != 7:
            raise ValueError("Not a UUIDv7")
        # Top 48 bits = Unix timestamp in milliseconds
        ts_ms = (u.int >> 80) & 0xFFFF_FFFF_FFFF
        return cls(datetime.fromtimestamp(ts_ms / 1000, tz=UTC))

    @classmethod
    def from_uuid7_desc(cls, u: UUID) -> Self:
        """Extract the original timestamp from a uuid7_desc UUID.

        Args:
            u: A UUID generated by uuid7_desc.

        Returns:
            The Timestamp embedded in the UUID.
        """
        inv_ts = (u.int >> 80) & 0xFFFF_FFFF_FFFF
        ts_ms  = (~inv_ts) & 0xFFFF_FFFF_FFFF
        return cls(datetime.fromtimestamp(ts_ms / 1000, tz=UTC))

    @classmethod
    def from_uuid(cls, u: UUID) -> Self:
        """Extract timestamp from a uuid7_desc UUID (newest-first ordering).

        Convenience alias for from_uuid7_desc.

        Args:
            u: A UUID generated by uuid7_desc or Timestamp.to_uuid().

        Returns:
            The Timestamp embedded in the UUID.
        """
        return cls.from_uuid7_desc(u)

    def to_uuid7(self) -> UUID:
        """Convert to a standard UUIDv7 (ascending lexicographic order)."""
        ts_ms = int(self.value.timestamp() * 1000)
        uuid_int  = ts_ms << 80
        uuid_int |= 0x7   << 76
        uuid_int |= 0b10  << 62
        uuid_int |= random.getrandbits(62)
        return UUID(int=uuid_int)

    def to_uuid7_desc(self) -> UUID:
        """Convert to a uuid7_desc UUID (inverted timestamp, newest-first ordering).

        Identical bit layout to UUIDv7 except the 48-bit millisecond timestamp is
        bitwise-inverted, so lexicographic ordering is newest-first.

        Returns:
            A UUID whose string representation sorts newest-first.
        """
        ts_ms  = int(self.value.timestamp() * 1000)
        inv_ts = (~ts_ms) & 0xFFFF_FFFF_FFFF
        rand_a = secrets.randbits(12)
        rand_b = secrets.randbits(62)
        return UUID(int=(
              (inv_ts << 80)
            | (0x7    << 76)
            | (rand_a << 64)
            | (0b10   << 62)
            |  rand_b
        ))

    def to_uuid(self) -> UUID:
        """Convert to a uuid7_desc UUID (newest-first ordering).

        Convenience alias for to_uuid7_desc.

        Returns:
            A UUID whose string representation sorts newest-first.
        """
        return self.to_uuid7_desc()

    def __str__(self) -> str:
        """String representation in ISO format."""
        return self.to_iso()

    def __repr__(self) -> str:
        """Developer-friendly representation."""
        return f"Timestamp({self.to_iso()})"


@define(slots=True)
class TimestampOrNone:
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
        """Extract timestamp from a standard UUIDv7.

        Args:
            u: A standard UUIDv7.

        Returns:
            The Timestamp embedded in the UUID.
        """
        if u.version != 7:
            raise ValueError("Not a UUIDv7")
        # Top 48 bits = Unix timestamp in milliseconds
        ts_ms = (u.int >> 80) & 0xFFFF_FFFF_FFFF
        return cls(datetime.fromtimestamp(ts_ms / 1000, tz=UTC))

    @classmethod
    def from_uuid7_desc(cls, u: UUID) -> Self:
        """Extract the original timestamp from a uuid7_desc UUID.

        Args:
            u: A UUID generated by uuid7_desc.

        Returns:
            The Timestamp embedded in the UUID.
        """
        inv_ts = (u.int >> 80) & 0xFFFF_FFFF_FFFF
        ts_ms  = (~inv_ts) & 0xFFFF_FFFF_FFFF
        return cls(datetime.fromtimestamp(ts_ms / 1000, tz=UTC))

    @classmethod
    def from_uuid(cls, u: UUID) -> Self:
        """Extract timestamp from a uuid7_desc UUID (newest-first ordering).

        Convenience alias for from_uuid7_desc.

        Args:
            u: A UUID generated by uuid7_desc or Timestamp.to_uuid().

        Returns:
            The Timestamp embedded in the UUID.
        """
        return cls.from_uuid7_desc(u)

    def to_uuid7(self) -> UUID:
        """Convert to a standard UUIDv7 (ascending lexicographic order)."""
        ts_ms = int(self.value.timestamp() * 1000)
        uuid_int  = ts_ms << 80
        uuid_int |= 0x7   << 76
        uuid_int |= 0b10  << 62
        uuid_int |= random.getrandbits(62)
        return UUID(int=uuid_int)

    def to_uuid7_desc(self) -> UUID:
        """Convert to a uuid7_desc UUID (inverted timestamp, newest-first ordering).

        Identical bit layout to UUIDv7 except the 48-bit millisecond timestamp is
        bitwise-inverted, so lexicographic ordering is newest-first.

        Returns:
            A UUID whose string representation sorts newest-first.
        """
        ts_ms  = int(self.value.timestamp() * 1000)
        inv_ts = (~ts_ms) & 0xFFFF_FFFF_FFFF
        rand_a = secrets.randbits(12)
        rand_b = secrets.randbits(62)
        return UUID(int=(
              (inv_ts << 80)
            | (0x7    << 76)
            | (rand_a << 64)
            | (0b10   << 62)
            |  rand_b
        ))

    def to_uuid(self) -> UUID:
        """Convert to a uuid7_desc UUID (newest-first ordering).

        Convenience alias for to_uuid7_desc.

        Returns:
            A UUID whose string representation sorts newest-first.
        """
        return self.to_uuid7_desc()

    def __str__(self) -> str:
        """String representation in ISO format."""
        return self.to_iso()

    def __repr__(self) -> str:
        """Developer-friendly representation."""
        return f"Timestamp({self.to_iso()})"


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
        """Convert to a PeriodUUID using uuid7_desc (newest-first ordering)."""
        return PeriodUUID(
            start=self.start.to_uuid() if self.start is not None else None,
            end=self.end.to_uuid() if self.end is not None else None,
        )

    def covers(self, ts: Timestamp) -> bool:
        return (
            (self.start is None or ts.value > self.start.value)
            and (self.end is None or ts.value < self.end.value)
        )


@define
class PeriodUUID:
    """A time range encoded as a pair of uuid7_desc UUIDs.

    Used as a compact, lexicographically sortable query key in storage paths
    and database range scans.  Because uuid7_desc inverts the timestamp bits,
    a newer UUID has a *smaller* integer value, so interval membership checks
    are reversed compared to standard uuid7 (see covers()).

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
        """Convert to a Period by extracting timestamps from uuid7_desc UUIDs."""
        return Period(
            start=Timestamp.from_uuid(self.start) if self.start is not None else None,
            end=Timestamp.from_uuid(self.end) if self.end is not None else None,
        )

    def covers(self, uid: UUID) -> bool:
        """Return True if uid falls within this period.

        With uuid7_desc, newer timestamps map to smaller UUID values (inverted
        ordering), so the comparisons are the reverse of standard uuid7:
        uid < start means uid is chronologically after start,
        uid > end   means uid is chronologically before end.
        """
        return (
            (self.start is None or uid < self.start)
            and (self.end is None or uid > self.end)
        )

# ---
# endregion


T = TypeVar("T")
R = TypeVar("R")
def map_or_none(value: T | None, func: Callable[[T], R]) -> R | None:
    return func(value) if value is not None else None
