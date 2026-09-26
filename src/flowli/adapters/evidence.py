"""The key layout of evidence, shared by the adapters. See specs/03-ports.md.

    wf/evidence/{eid}/{frame}/meta        {"fid": ..., "attempt": ...}
    wf/evidence/{eid}/{frame}/log.{part}  one flush of the attempt log
    wf/evidence/{eid}/{frame}/a/{name}    one attachment
    wf/evidence/{eid}/{frame}/mt/{name}   that attachment's media type

`{frame}` is `{digest_text(fid)[:16]}.{attempt}`. A frame id holds `/`, `#`
and `:`, so it is not put in a key. The map back to the frame id is the
`meta` object.

`mt/{name}` lives in its own segment, not alongside `a/{name}`, so a real
attachment can never be named e.g. `"foo.type"` and collide with the media
type sidecar for an attachment named `"foo"`.
"""

from __future__ import annotations

import json
import re

from flowli.domain import Eid, EvidenceRef, InvalidName, parse_eid
from flowli.domain.names import digest_text

PREFIX = "wf/evidence"
LOG_NAME = "log"
NDJSON = "application/x-ndjson"
_NAME_RE = re.compile(r"^[A-Za-z0-9._-]{1,128}$")


def check_attachment_name(name: str) -> str:
    """An attachment name is one key segment, never a path."""
    if name == LOG_NAME or not _NAME_RE.match(name) or name.startswith("."):
        raise InvalidName(f"invalid attachment name {name!r}")
    return name


def frame_segment(fid: str, attempt: int) -> str:
    return f"{digest_text(fid)[:16]}.{attempt}"


def base(ref: EvidenceRef) -> str:
    return f"{PREFIX}/{ref.eid}/{frame_segment(ref.fid, ref.attempt)}"


def eid_prefix(eid: Eid) -> str:
    return f"{PREFIX}/{eid}/"


def meta_key(ref: EvidenceRef) -> str:
    return f"{base(ref)}/meta"


def log_key(ref: EvidenceRef, part: int) -> str:
    return f"{base(ref)}/{LOG_NAME}.{part:06d}"


def attachment_key(ref: EvidenceRef, name: str) -> str:
    return f"{base(ref)}/a/{check_attachment_name(name)}"


def media_type_key(ref: EvidenceRef, name: str) -> str:
    return f"{base(ref)}/mt/{check_attachment_name(name)}"


def meta_bytes(ref: EvidenceRef) -> bytes:
    return json.dumps({"fid": ref.fid, "attempt": ref.attempt}, sort_keys=True).encode()


def ref_from_meta(eid: Eid | str, data: bytes) -> EvidenceRef:
    doc = json.loads(data)
    return EvidenceRef(parse_eid(eid), str(doc["fid"]), int(doc["attempt"]))


def tail_of(key: str, prefix: str) -> str:
    return key[len(prefix) :] if key.startswith(prefix) else key
