from pathlib import Path
from typing import TYPE_CHECKING, Protocol
from uuid import UUID

import attrs


# region @storage.types.stat
# ---
# role: storage
# intent: stat_result-compatible protocol satisfied by os.stat_result and AzureBlobStat
# description: >
#   StatLike mirrors the subset of os.stat_result used across the codebase.
#   os.stat_result (returned by Path.stat()) satisfies it directly via typeshed.
#   AzureBlobStat (returned by AzureBlobPath.stat()) satisfies it explicitly.
# ---

class StatLike(Protocol):
    """Protocol satisfied by os.stat_result and AzureBlobStat."""
    st_size: int
    st_mtime: float
    st_ctime: float
    st_atime: float

# ---
# endregion


# region @storage.types.storagepath
# ---
# role: storage
# intent: union of all registered storage-path types; extend when adding new backends
# description: >
#   StoragePath is the concrete union of every path-like type supported by the
#   storage layer (Path, AzureBlobPath, …). All members must expose the same
#   interface as pathlib.Path so callers stay backend-agnostic.
#   AzureBlobPath is optional: when azure-storage-blob is absent the alias falls
#   back to Path alone and the module imports cleanly.
# rules:
#   - To add a backend, you MUST append its type to this union and implement
#     the Path interface
#   - You SHOULD prefer StoragePath at API boundaries over concrete types
# ---

if TYPE_CHECKING:
    from .azure import AzureBlobPath
    StoragePath = Path | AzureBlobPath
else:
    try:
        from .azure import AzureBlobPath as _AzureBlobPath
        StoragePath = Path | _AzureBlobPath
    except ImportError:
        StoragePath = Path


def is_remote(path: StoragePath) -> bool:
    return isinstance(path, Path)

# ---
# endregion


# region @storage.types.logpath
# ---
# role: storage
# intent: structured log-file path following the /runs/<name>/<uuid>.jsonl schema
# description: >
#   LogPath couples a StoragePath with the name and id parsed from it.
#   path is the single source of truth; name and id are derived in __attrs_post_init__.
#   Use from_path() to parse an existing path and build() to construct a new one.
# rules:
#   - path MUST match …/runs/<name>/<uuid>.jsonl; raises ValueError otherwise
#   - name and id MUST NOT be set directly; mutate path and re-parse instead
# dependencies:
#   - storage.types.storagepath
# ---


@attrs.define(slots=True, kw_only=True)
class LogPath:
    """Log-file path structured under the ``/runs/<name>/<uuid>.jsonl`` schema.

    ``path`` is the single source of truth. ``name`` and ``id`` are derived
    from it on construction and stored for fast access.

    Attributes:
        path: Full storage path to the log file.
        name: Run name extracted from the second-to-last path component.
        id: Run UUID extracted from the file stem.
    """

    path: StoragePath = attrs.field()
    name: str = attrs.field(init=False)
    id: UUID = attrs.field(init=False)

    @path.validator
    def check(self, _, value):
        parts = value.parts
        if not value.suffix == ".jsonl" or len(parts) < 3 or parts[-3] != "runs":
            raise ValueError(
                f"{self.path!r} does not match /runs/<name>/<uuid>.jsonl"
            )

    def __attrs_post_init__(self) -> None:
        self.name = self.path.parts[-2]
        self.id = UUID(self.path.stem)  # raises ValueError if stem is not a valid UUID

    @classmethod
    def build(cls, root: StoragePath, name: str, id: UUID) -> "LogPath":
        """Construct a LogPath from a root directory, run name, and UUID.

        Args:
            root: Root storage path.
            name: Run name.
            id: Run UUID.

        Returns:
            A LogPath pointing to ``root/runs/<name>/<id>.jsonl``.
        """
        return cls(path=root / "runs" / name / f"{id}.jsonl")

# ---
# endregion


# region @storage.types.statepath
# ---
# role: storage
# intent: structured state-file path following the /state/<flow_name>/<run_id>.json schema
# description: >
#   StatePath couples a StoragePath with the flow_name and run_id parsed from it.
#   path is the single source of truth; flow_name and run_id are derived in
#   __attrs_post_init__. Use build() to construct a new path and from_path() to
#   parse an existing one.
# rules:
#   - path MUST match …/state/<flow_name>/<run_id>.json; raises ValueError otherwise
#   - flow_name and run_id MUST NOT be set directly; mutate path and re-parse instead
# dependencies:
#   - storage.types.storagepath
# ---


@attrs.define(slots=True, kw_only=True)
class StatePath:
    """State-file path structured under the ``/state/<flow_name>/<run_id>.json`` schema.

    ``path`` is the single source of truth. ``flow_name`` and ``run_id`` are
    derived from it on construction and stored for fast access.

    Attributes:
        path: Full storage path to the state file.
        flow_name: Flow name extracted from the second-to-last path component.
        run_id: Run UUID extracted from the file stem.
    """

    path: StoragePath = attrs.field()
    flow_name: str = attrs.field(init=False)
    run_id: UUID = attrs.field(init=False)

    @path.validator
    def _check(self, _, value: StoragePath) -> None:
        parts = value.parts
        if value.suffix != ".json" or len(parts) < 3 or parts[-3] != "state":
            raise ValueError(
                f"{value!r} does not match /state/<flow_name>/<run_id>.json"
            )

    def __attrs_post_init__(self) -> None:
        self.flow_name = self.path.parts[-2]
        self.run_id = UUID(self.path.stem)

    @classmethod
    def build(cls, root: StoragePath, flow_name: str, run_id: UUID) -> "StatePath":
        """Construct a StatePath from a root directory, flow name, and UUID.

        Args:
            root: Root storage path.
            flow_name: Name of the flow.
            run_id: Run UUID.

        Returns:
            A StatePath pointing to ``root/state/<flow_name>/<run_id>.json``.
        """
        return cls(path=root / "state" / flow_name / f"{run_id}.json")

    @classmethod
    def from_path(cls, path: StoragePath) -> "StatePath":
        """Parse a StatePath from an existing storage path.

        Args:
            path: Storage path matching the state schema.

        Returns:
            A StatePath with derived flow_name and run_id.

        Raises:
            ValueError: If the path does not match the expected schema.
        """
        return cls(path=path)

# ---
# endregion
