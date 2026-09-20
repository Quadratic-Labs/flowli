"""The merge queue. Spec 11 section 7.

One writer to the integration branch. A conflict or a failed gate is an
answer, and the queue keeps going.
"""

from __future__ import annotations

from pathlib import Path

from flowlet.domain import DelegateTask
from flowlet_runner import worktree as wt

from flowlet_codeflow import MergeConfig, MergeHandler

from .conftest import git


def branch_with(repo, name: str, path: str, text: str) -> str:
    """A branch off main that changes one file."""
    tree, _, _ = wt.create(repo, eid="e", key=name, base_ref="main")
    Path(tree, path).parent.mkdir(parents=True, exist_ok=True)
    Path(tree, path).write_text(text)
    git(tree, "add", "-A")
    git(tree, "-c", "user.email=a@b", "-c", "user.name=A", "commit", "-qm", f"work {name}")
    git(repo, "branch", "-f", name, git(tree, "rev-parse", "HEAD"))
    wt.remove(repo, tree)
    return name


class FakeHeld:
    """Just enough of `Held` for the handler: it reads the task payload only."""

    def __init__(self, branch: str) -> None:
        self.task = DelegateTask(
            eid="e", fid="root", reply_channel="c", payload={"branch": branch}
        )
        self.task_id = f"merge:{branch}"


async def merge(repo, branch: str, **config) -> dict:
    handler = MergeHandler(MergeConfig(repo_path=str(repo), integration_branch="main", **config))
    held = FakeHeld(branch)
    handle = await handler.start(held)  # type: ignore[arg-type]
    return await handler.poll(held, handle)  # type: ignore[arg-type]


async def test_a_clean_branch_reaches_the_integration_branch(repo):
    branch_with(repo, "b1", "src/a.py", "one\n")
    before = git(repo, "rev-parse", "main")

    result = await merge(repo, "b1")

    assert result["merged"] is True
    assert git(repo, "rev-parse", "main") == result["commit"] != before
    assert "one" in git(repo, "show", "main:src/a.py")


async def test_two_branches_merge_one_after_the_other(repo):
    """One consumer holds one task at a time, so this is the order of the
    queue and not a race."""
    branch_with(repo, "b1", "src/a.py", "one\n")
    branch_with(repo, "b2", "src/b.py", "two\n")

    assert (await merge(repo, "b1"))["merged"]
    assert (await merge(repo, "b2"))["merged"]

    assert "one" in git(repo, "show", "main:src/a.py")
    assert "two" in git(repo, "show", "main:src/b.py")


async def test_a_conflict_is_an_answer_and_leaves_the_branch_alone(repo):
    branch_with(repo, "b1", "src/same.py", "from one\n")
    branch_with(repo, "b2", "src/same.py", "from two\n")
    assert (await merge(repo, "b1"))["merged"]
    head = git(repo, "rev-parse", "main")

    result = await merge(repo, "b2")

    assert result["merged"] is False
    assert result["reason"] == "conflict"
    assert result["conflicts"] == ["src/same.py"]
    # The queue is not blocked by one bad merge, and nothing was published.
    assert git(repo, "rev-parse", "main") == head
    assert (await merge(repo, "b1"))["merged"] is True  # it still works after


async def test_a_failed_gate_stops_the_merge_before_it_is_published(repo):
    branch_with(repo, "b1", "src/a.py", "one\n")
    head = git(repo, "rev-parse", "main")

    result = await merge(repo, "b1", gates=("test -f nothing-here",))

    assert result["merged"] is False
    assert result["reason"] == "a merge gate failed"
    assert result["gate"]["command"] == "test -f nothing-here"
    assert git(repo, "rev-parse", "main") == head


async def test_a_post_merge_failure_is_reported_after_the_fact(repo):
    """The gates before the merge decide; the ones after it inform."""
    branch_with(repo, "b1", "src/a.py", "one\n")

    result = await merge(repo, "b1", post_merge=("false",))

    assert result["merged"] is True
    assert result["post_merge_failed"]["command"] == "false"
    assert git(repo, "rev-parse", "main") == result["commit"]


async def test_a_merge_leaves_no_worktree_behind(repo):
    branch_with(repo, "b1", "src/a.py", "one\n")
    await merge(repo, "b1")
    assert not list((Path(repo) / "mq").glob("*")) or not any(
        p.is_dir() and (p / ".git").exists() for p in (Path(repo) / "mq").iterdir()
    )


async def test_a_request_without_a_branch_is_refused(repo):
    assert (await merge(repo, ""))["reason"] == "no branch"
