"""One attempt writes in one place. Spec 10 section 7."""

from pathlib import Path

from flowlet_runner import worktree as wt

from .conftest import git

EID = "01a08f9f-71b4-7085-a9c1-cb0b10fe1db1"


def test_create_carves_a_branch_named_after_the_frame(repo):
    path, branch, base = wt.create(repo, eid=EID, key="1", base_ref="main")

    assert branch == f"agent/{EID}/1"
    assert Path(path, "README.md").read_text() == "hello\n"
    assert base == git(repo, "rev-parse", "main")


def test_a_repeat_of_one_attempt_finds_its_own_worktree(repo):
    first = wt.create(repo, eid=EID, key="1", base_ref="main")
    second = wt.create(repo, eid=EID, key="1", base_ref="main")
    assert first == second  # derived names converge; nothing is minted


def test_two_attempts_are_two_branches(repo):
    _, one, _ = wt.create(repo, eid=EID, key="1", base_ref="main")
    _, two, _ = wt.create(repo, eid=EID, key="2", base_ref="main")
    assert one != two


def test_the_commits_of_an_attempt_are_its_result(repo):
    path, _, base = wt.create(repo, eid=EID, key="1", base_ref="main")
    Path(path, "NOTES.md").write_text("did the thing\n")
    git(path, "add", "-A")
    git(path, "commit", "-qm", "work")

    commits = wt.new_commits(path, base)
    assert len(commits) == 1
    assert git(repo, "log", "-1", "--format=%s", commits[0]) == "work"


def test_remove_keeps_the_branch(repo):
    path, branch, _ = wt.create(repo, eid=EID, key="1", base_ref="main")
    wt.remove(repo, path)

    assert not Path(path).exists()
    assert branch in git(repo, "branch", "--list", branch)


def test_a_killed_attempt_is_quarantined_not_deleted(repo):
    """A person can salvage it (section 7)."""
    path, _, _ = wt.create(repo, eid=EID, key="1", base_ref="main")
    Path(path, "half-done.txt").write_text("partial\n")

    bundle = wt.quarantine(repo, path, name=f"{EID}-1")

    assert bundle and Path(bundle).exists()
    assert not Path(path).exists()
    # The bundle is a real repository: the work is recoverable.
    heads = git(repo, "bundle", "list-heads", bundle)
    assert heads  # a real bundle with a head to restore from
