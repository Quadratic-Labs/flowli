"""The codec: dict forms, the canonical JSON, and the argument digest."""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import pytest
from cairndb import Timestamp
from hypothesis import given
from hypothesis import strategies as st

from flowli.adapters.cairndb import ISO_WIDTH
from flowli.codec import canonical_json, digest, structure, unstructure
from flowli.domain import InvalidName, RetryPolicy, digest_text
from tests.ids import E_ABC


def test_iso_width_matches_cairndb_timestamp_form():
    assert len(Timestamp(datetime(2026, 9, 7, 9, 11, 0, 123456, tzinfo=UTC)).to_iso()) == ISO_WIDTH
    assert len(Timestamp(datetime(2026, 1, 1, tzinfo=UTC)).to_iso()) == ISO_WIDTH


def test_value_types_have_one_rendering():
    ts = Timestamp(datetime(2026, 9, 7, tzinfo=UTC))
    assert unstructure(ts) == "2026-09-07T00:00:00.000000Z"
    assert unstructure(E_ABC) == str(E_ABC)
    assert unstructure(timedelta(seconds=90)) == 90.0
    assert structure("2026-09-07T00:00:00.000000Z", Timestamp) == ts
    assert structure(ts.value, Timestamp) == ts  # a datetime is accepted inbound
    assert structure(str(E_ABC), type(E_ABC)) == E_ABC
    with pytest.raises(InvalidName):
        structure("nope", type(E_ABC))
    p = RetryPolicy(max_attempts=2, backoff=timedelta(seconds=1.5))
    assert unstructure(p) == {
        "max_attempts": 2,
        "backoff": 1.5,
        "backoff_factor": 1.0,
        "max_backoff": None,
    }
    assert structure(unstructure(p), RetryPolicy) == p


def test_structure_timestamp_rejects_unsupported_type():
    with pytest.raises(TypeError, match="int"):
        structure(123, Timestamp)


def test_canonical_json_is_order_independent_and_json_native_only():
    assert canonical_json({"b": 1, "a": [1, {"d": 2, "c": 3}]}) == canonical_json(
        {"a": [1, {"c": 3, "d": 2}], "b": 1}
    )
    with pytest.raises(TypeError):
        canonical_json({"at": Timestamp.now()})  # render with unstructure first


def test_canonical_json_is_compact_ascii_preserving_and_rejects_nan():
    assert canonical_json({"a": 1, "b": 2}) == '{"a":1,"b":2}'  # sorted, no whitespace
    assert canonical_json({"name": "café"}) == '{"name":"café"}'  # literal UTF-8, not \u-escaped
    with pytest.raises(ValueError):
        canonical_json(float("nan"))  # no NaN


@dataclass(frozen=True)
class Invoice:
    id: str
    amount: int


def test_digest_renders_through_the_codec():
    ts = Timestamp(datetime(2026, 9, 7, tzinfo=UTC))
    assert digest({"at": ts}) == digest({"at": "2026-09-07T00:00:00.000000Z"})
    assert digest({"at": ts.value}) == digest({"at": ts})
    assert digest([E_ABC]) == digest([str(E_ABC)])
    assert digest(Invoice("i-1", 10)) == digest({"id": "i-1", "amount": 10})
    assert digest(Invoice("i-1", 10)) != digest(Invoice("i-1", 11))


def test_digest_text_is_plain_string_hashing():
    assert digest_text("root/a#0") == digest_text("root/a#0")
    assert len(digest_text("x")) == 64 and digest_text("x") != digest_text("y")


json_scalars = st.none() | st.booleans() | st.integers() | st.text()
json_values = st.recursive(
    json_scalars,
    lambda inner: st.lists(inner) | st.dictionaries(st.text(), inner),
    max_leaves=20,
)


@given(json_values)
def test_digest_is_deterministic(value):
    assert digest(value) == digest(value)
    assert len(digest(value)) == 64
