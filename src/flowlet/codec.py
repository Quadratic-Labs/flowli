"""Serialization of domain objects, in one place, outside the domain. Also the content
digest of values, since hashing a value means rendering it first.

The domain dataclasses describe data and check intrinsic invariants only. Turning them
into JSON-compatible dicts and back is this module's job, through a cattrs converter
with hooks for the three value types that are not JSON-native:

    Timestamp  <->  RFC 3339 string (Timestamp.to_iso / from_iso)
    UUID       <->  canonical string
    timedelta  <->  seconds (float)
    datetime    ->  RFC 3339 string, as a Timestamp (user data in step arguments)

`unstructure(obj)` gives the dict form. `structure(data, cls)` parses it back, and is
where inbound values are validated and coerced. Adapters, the engine and the patterns
use these two functions. Nothing in `flowlet.domain` imports this module.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timedelta
from enum import Enum
from typing import Any

from cairndb import Timestamp
from cattrs import Converter

from flowlet.domain import parse_eid

# detailed_validation off: a hook's InvalidName reaches the caller as is, not wrapped.
converter = Converter(detailed_validation=False)


def _structure_timestamp(value: Any, _: Any) -> Timestamp:
    if isinstance(value, Timestamp):
        return value
    if isinstance(value, str):
        return Timestamp.from_iso(value)
    if isinstance(value, datetime):
        return Timestamp(value)
    raise TypeError(f"cannot read a Timestamp from {type(value).__name__}")


converter.register_unstructure_hook(Timestamp, lambda t: t.to_iso())
converter.register_structure_hook(Timestamp, _structure_timestamp)
converter.register_unstructure_hook(datetime, lambda d: Timestamp(d).to_iso())  # user data
converter.register_unstructure_hook(uuid.UUID, str)
converter.register_structure_hook(uuid.UUID, lambda v, _: parse_eid(v))
converter.register_unstructure_hook(timedelta, lambda d: d.total_seconds())
converter.register_unstructure_hook_func(
    lambda cls: isinstance(cls, type) and issubclass(cls, Enum), lambda e: e.value
)
converter.register_structure_hook(timedelta, lambda v, _: timedelta(seconds=float(v)))


def unstructure(obj: Any) -> Any:
    """The JSON-compatible form of a domain object (dict, list, or scalar)."""
    return converter.unstructure(obj)


def structure[T](data: Any, cls: type[T]) -> T:
    """Parse the JSON-compatible form back into `cls`. Validates and coerces on the way."""
    return converter.structure(data, cls)


def canonical_json(data: Any) -> str:
    """Deterministic JSON of JSON-native data: sorted keys, no whitespace, no NaN, UTF-8.

    Render domain values with `unstructure` first. Anything else raises TypeError.
    """
    return json.dumps(
        data, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    )  # pragma: no mutate


def digest(value: Any) -> str:
    """sha256 hex of the canonical JSON of `unstructure(value)`.

    The frame argument digest of the memo rule (02-journal.md section 5). One rendering
    rule set, the codec's, decides how a Timestamp, a UUID or a dataclass looks here.
    """
    return hashlib.sha256(canonical_json(unstructure(value)).encode("utf-8")).hexdigest()  # pragma: no mutate


__all__ = ["canonical_json", "converter", "digest", "structure", "unstructure"]
