"""Wire format for flowlet domain models, via a configured cattrs converter.

The attrs models are the single source of truth for field names, defaults,
optionality, and nesting; the converter only adds the scalar encodings
(UUID <-> str, Timestamp <-> ISO 8601 — StrEnums encode natively) and the
wire contract's tolerance for explicit ``null`` on defaulted fields.
"""

import json
from collections.abc import Callable
from typing import Any
from uuid import UUID

import attrs
from cattrs import Converter
from cattrs.gen import make_dict_structure_fn

from flowlet.types import Timestamp

converter = Converter()

converter.register_unstructure_hook(UUID, str)
converter.register_structure_hook(UUID, lambda v, _: UUID(v))
converter.register_unstructure_hook(Timestamp, Timestamp.to_iso)
converter.register_structure_hook(Timestamp, lambda v, _: Timestamp.from_iso(v))


@converter.register_structure_hook_factory(attrs.has)
def _null_tolerant_hook(cl: type, conv: Converter) -> Callable:
    """Treat an explicit ``null`` like an absent key for defaulted fields.

    Hand-written or older documents may carry ``"attributes": null`` where
    the model wants ``{}``; the previous hand-rolled parsers were lenient
    about this (``raw.get(...) or {}``), so the wire contract keeps that
    tolerance. Unknown keys are ignored, as before.
    """
    base = make_dict_structure_fn(cl, conv)
    defaulted = {f.name for f in attrs.fields(cl) if f.default is not attrs.NOTHING}

    def hook(data: dict, _: Any):
        cleaned = {
            k: v for k, v in data.items() if not (v is None and k in defaulted)
        }
        return base(cleaned, cl)

    return hook


def destructure(data: Any) -> Any:
    """Convert a model (or list/dict of models) to a JSON-safe tree."""
    return converter.unstructure(data)


def structure(model: type) -> Callable[[Any], Any]:
    """Curried inverse of :func:`destructure`: ``structure(ObligationSummary)(tree)``."""
    return lambda tree: converter.structure(tree, model)


def to_json(data: Any) -> str:
    """Serialize a domain model to its wire-format JSON string."""
    return json.dumps(converter.unstructure(data))


def from_json(model: type) -> Callable[[str], Any]:
    """Curried deserializer: ``from_json(ObligationSummary)(text)``."""
    return lambda text: converter.structure(json.loads(text), model)


def to_payload(data: Any) -> Any:
    """Convert a domain model to a JSON-safe tree (the wire format as data).

    Used where a model travels inside another JSON document — e.g. the
    obligation record as the state payload of a cairndb lease document —
    so the embedded form is byte-equivalent to the ``to_json`` wire format.
    """
    return converter.unstructure(data)


def from_payload(model: type) -> Callable[[Any], Any]:
    """Curried inverse of :func:`to_payload`: ``from_payload(ObligationSummary)(tree)``."""
    return lambda payload: converter.structure(payload, model)
