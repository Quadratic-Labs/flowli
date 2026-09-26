"""Git worktree isolation. See specs/10-agent-runner.md section 7.

One attempt writes in one place. The commits on its branch are the result of
the attempt, and the report carries their SHAs. Nothing merges here: a merge is
a later frame of the workflow, with one writer.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

from flowli.log import get_logger

log = get_logger("flowli_runner.worktree")


def git(repo: str | Path, *args: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(repo), *args], capture_output=True, text=True, check=True
    )
    return result.stdout.strip()


def branch_name(eid: str, key: str) -> str:
    """`agent/{eid}/{key}`. The name comes from the frame, so it is
    deterministic and a repeat converges on the same branch."""
    return f"agent/{eid}/{key}"


def create(repo: str | Path, *, eid: str, key: str, base_ref: str = "HEAD") -> tuple[str, str, str]:
    """Carve the worktree of one attempt. Returns (path, branch, base_commit).

    Idempotent: a repeat of the same attempt finds its own worktree and uses
    it again, because the branch name is derived and not minted.
    """
    root = Path(repo).resolve()
    branch = branch_name(eid, key)
    path = str(root / "wt" / eid / key)

    if Path(path, ".git").exists():
        base = git(path, "rev-parse", branch)
        log.info("worktree_reused", path=path, branch=branch)
        return path, branch, base

    base = git(root, "rev-parse", base_ref)
    if branch in git(root, "branch", "--list", branch):
        git(root, "worktree", "add", path, branch)
    else:
        git(root, "worktree", "add", path, "-b", branch, base)
    log.info("worktree_created", path=path, branch=branch, base=base)
    return path, branch, base


def remove(repo: str | Path, path: str) -> None:
    """Remove the worktree. The branch and its commits stay."""
    if Path(path).exists():
        git(repo, "worktree", "remove", "--force", path)


def new_commits(worktree: str | Path, base_commit: str) -> list[str]:
    """The SHAs this attempt added on top of its base, newest first."""
    try:
        out = git(worktree, "rev-list", f"{base_commit}..HEAD")
    except subprocess.CalledProcessError:
        return []
    return [sha for sha in out.splitlines() if sha]


def quarantine(repo: str | Path, path: str, *, name: str) -> str | None:
    """Keep a killed attempt's tree as a bundle, then remove the worktree.

    A killed attempt is not deleted: a person can salvage it (section 7).
    """
    root = Path(repo).resolve()
    bundle = root / "quarantine" / f"{name}.bundle"
    bundle.parent.mkdir(parents=True, exist_ok=True)
    try:
        git(path, "add", "-A")
        try:
            git(path, "commit", "--allow-empty", "-m", f"quarantine {name}")
        except subprocess.CalledProcessError:
            log.warning("quarantine_commit_failed", path=path)
        git(path, "bundle", "create", str(bundle), "HEAD")
    except subprocess.CalledProcessError:
        log.warning("quarantine_failed", path=path)
        return None
    finally:
        with _suppress():
            remove(repo, path)
    log.info("worktree_quarantined", path=path, bundle=str(bundle))
    return str(bundle)


class _suppress:
    def __enter__(self) -> None:
        return None

    def __exit__(self, *exc: object) -> bool:
        return True


def discard(path: str) -> None:
    """Remove a directory that git no longer tracks."""
    shutil.rmtree(path, ignore_errors=True)


__all__ = ["branch_name", "create", "discard", "git", "new_commits", "quarantine", "remove"]
