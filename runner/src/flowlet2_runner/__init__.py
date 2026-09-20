"""Run coding agents as the consumer of flowlet delegate tasks.

See `flowlet/docs/specs/10-agent-runner.md`. The loop, the ownership of a long
attempt, the recovery and the cancel are `flowlet.runtime.Consumer`; this
package is the work it drives: adapters, worktrees, write scopes, and the two
contracts an agent task carries.
"""

from .adapters import AgentAdapter, Handle, ServiceAgent, SubprocessAgent
from .envelope import Envelope, Report, Worktree
from .runner import AgentRunner, RunnerConfig, build_consumer

__all__ = [
    "AgentAdapter",
    "AgentRunner",
    "Envelope",
    "Handle",
    "Report",
    "RunnerConfig",
    "ServiceAgent",
    "SubprocessAgent",
    "Worktree",
    "build_consumer",
]
