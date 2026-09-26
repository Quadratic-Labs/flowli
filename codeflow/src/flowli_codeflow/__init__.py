"""The CodeFlow controller. See `flowli/specs/11-codeflow.md`.

The controller is workflows: a milestone is an execution, a feature is a child
of it, and a task is a child of a feature. Three things stay processes,
because they never end — the agent runner (`flowli_runner`), the merge queue
(`merge.py`) and the board reconciler (`board.py`).
"""

from .board import Board, Item, MemoryBoard, Reconciler
from .envelope import build_envelope, summarize
from .gates import run_gates
from .gotchas import Gotcha, Registry
from .merge import MergeConfig, MergeHandler
from .merge import build_consumer as merge_consumer
from .model import Applied, FeatureSpec, Milestone, PlanError, TaskSpec, apply_delta, check_feature
from .routing import Decision, Route, decide
from .workflows import Policy, register

__all__ = [
    "Applied",
    "Board",
    "Decision",
    "FeatureSpec",
    "Gotcha",
    "Item",
    "MemoryBoard",
    "MergeConfig",
    "MergeHandler",
    "Milestone",
    "PlanError",
    "Policy",
    "Reconciler",
    "Registry",
    "Route",
    "TaskSpec",
    "apply_delta",
    "build_envelope",
    "check_feature",
    "decide",
    "merge_consumer",
    "register",
    "run_gates",
    "summarize",
]
