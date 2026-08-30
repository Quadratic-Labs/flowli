"""
Unit tests for the API DTO layer (pydantic models over the wire format).

Covers the seam between serdes and the DTOs: ``RunQuery.get_run`` returns a
*destructured* tree (timestamps as ISO 8601 strings), which the controller
validates with ``TraceDTO.model_validate``.  A validation failure here is
silently downgraded to a 404 by the controller (ValidationError extends
ValueError), so this seam needs its own regression coverage.
"""
from datetime import UTC, datetime

import pytest

from flowlet.api.models import TraceDTO, TimestampDTO
from flowlet.models import ReportedStatus, TraceSummary
from flowlet.serdes import destructure
from flowlet.types import Timestamp
from pydantic import BaseModel


class _TsHolder(BaseModel):
    ts: TimestampDTO


@pytest.mark.unit
class TestTimestampDTO:
    """TimestampDTO validates every documented input form to a Timestamp —
    the server-side type carrying the UTC contract — and serialises to ISO."""

    def test_accepts_timestamp_object(self, make_ts):
        ts = make_ts()
        assert _TsHolder(ts=ts).ts is ts

    def test_accepts_datetime(self):
        dt = datetime(2026, 8, 23, 19, 23, 34, tzinfo=UTC)
        assert _TsHolder(ts=dt).ts == Timestamp(dt)

    def test_accepts_iso_string(self):
        # The serdes wire format: destructure() emits Timestamp as ISO 8601.
        holder = _TsHolder(ts="2026-08-23T19:23:34.396769Z")
        assert holder.ts == Timestamp(
            datetime(2026, 8, 23, 19, 23, 34, 396769, tzinfo=UTC)
        )

    def test_naive_datetime_becomes_utc(self):
        holder = _TsHolder(ts=datetime(2026, 8, 23, 19, 23, 34))
        assert holder.ts.value.tzinfo is UTC

    def test_serialises_to_canonical_iso(self, make_ts):
        ts = make_ts()
        assert _TsHolder(ts=ts).model_dump_json() == f'{{"ts":"{ts.to_iso()}"}}'

    def test_rejects_other_types(self):
        with pytest.raises(ValueError):
            _TsHolder(ts=12345)


@pytest.mark.unit
class TestRunDTOFromWireFormat:
    """TraceDTO validates the destructured tree that RunQuery.get_run returns."""

    def test_destructured_run_validates(self, make_span_record, make_ts):
        # Regression: after the serdes-on-cattrs refactor, get_run returns
        # ISO-string timestamps; TraceDTO rejected them, and the controller's
        # except ValueError turned that into a bogus 404.
        summary = TraceSummary(
            span_id="c2e23227ee137844",
            span_name="simple_etl",
            span_type="flow",
            status=ReportedStatus.completed,
            start_ts=make_ts(),
            end_ts=make_ts(),
        )
        result = destructure(summary)
        result["logs"] = destructure([make_span_record()])

        dto = TraceDTO.model_validate(result)

        assert dto.status == "completed"
        assert dto.logs[0].start_ts == Timestamp.from_iso(result["logs"][0]["start_ts"])
