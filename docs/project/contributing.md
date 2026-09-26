# Contributing

Contributions are welcome: bug reports, fixes, documentation, and
feedback from running workflows on Flowli. See the [Roadmap](roadmap.md)
for where help matters most. Everyone taking part is expected to follow
the [Code of Conduct](code-of-conduct.md).

## Reporting bugs

Open an issue on [GitHub](https://github.com/Quadratic-Labs/flowli/issues)
with the Flowli and CairnDB versions, the storage backend, and a minimal
reproduction. A workflow that reproduces on the in-memory backend
(`flowli.adapters.memory.MemoryBackend`) is the easiest to act on. For an
execution that went wrong, attach the output of
`flowli status EID --journal`.

## Setup

Flowli needs Python 3.14 and [uv](https://docs.astral.sh/uv/):

```bash
git clone git@github.com:Quadratic-Labs/flowli.git
cd flowli
uv sync --all-extras
uv pip install -e runner -e codeflow   # only to work on these two packages
```

The `dev` extra holds the test tools, FastAPI and the HTTP client, so the
tests of the HTTP service run too.

## Common tasks

| Command | What it does |
|---|---|
| `uv run pytest` | the tests of `flowli` (`tests/`) |
| `uv run pytest runner/tests` / `uv run pytest codeflow/tests` | the tests of the runner and of CodeFlow |
| `uv run ruff check .` | lint |
| `uv run mypy src` | type-check (strict) |
| `uv run mutmut run` | mutation testing |
| `make -C docs html` / `make -C docs livehtml` | build this documentation / live-reload it |

`uv run` syncs the environment first. Pass `--all-extras` to it, or call
the tools from `.venv/bin/`, so the sync keeps the extras installed.

## Test design

- `tests/unit/` runs against the in-memory backend. Its clock is
  injectable (`ManualClock`), so timers, leases and retention run without
  waiting.
- `tests/integration/` runs against CairnDB on the filesystem backend,
  whose put-if-absent is atomic, so the concurrency tests
  (`test_concurrency.py`) exercise real contention without cloud
  credentials. `sample_app.py` and `concurrent_app.py` are the registries
  that the CLI tests load with `--app`.
- `tests/api/` drives the HTTP service in process.

## Code layout

```text
src/flowli/
├── domain/     # pure domain objects and ports: no I/O, no CairnDB import
├── runtime/    # Engine, Context, Registry, worker, sweeper, retention
├── patterns/   # delegate, fanout, review, saga, schedule: on the API only
├── adapters/   # port implementations: CairnDB, in-memory
├── api/        # the HTTP service (the api extra)
└── cli.py      # the flowli command (the cli extra)
runner/         # flowli-runner: coding agents as delegate consumers
codeflow/       # flowli-codeflow: the controller over the runner
web/            # the operator interface over the HTTP service
specs/          # the specifications: the authority on behaviour
```

## Invariants: do not break these

The [specifications](https://github.com/Quadratic-Labs/flowli/tree/main/specs)
state them in full (`specs/00-overview.md`, section 3):

1. One journal per execution. It is both the trace and the memo table.
2. The first `frame.completed` entry for a frame id wins. Later ones are
   ignored, so duplicate execution converges.
3. Replay runs the workflow from the top. A frame with a memo returns it
   without running.
4. Frame ids are deterministic paths. They never depend on wall-clock
   time, on the worker, or on completion order.
5. Exactly one worker owns an execution, by lease. Every write carries the
   lease epoch as a fence token.
6. Every write carries its provenance: who acted, what code, where.
7. The shared control log receives lifecycle events only.

A change of behaviour starts with a change of the specification.

## Writing documentation

The docs are Markdown ([MyST](https://myst-parser.readthedocs.io/)) built
with Sphinx. The API reference is generated from docstrings, so
**document behaviour in the docstring**, in Google style (`Args:`,
`Returns:`, `Raises:`).

```bash
uv sync --all-extras
make -C docs html        # builds docs/_build/html, and fails on warnings
make -C docs livehtml    # live-reloading server
```

- The pages use ASD-STE100 Strict, like the specifications: one word for
  one concept, short sentences, the active voice.
- Code samples should run.
- New pages must be added to a `toctree` in `docs/index.md`.


## Releasing

Releases combine two kinds of Git refs:

- **Release branches** (`release/X.Y`) are lines of maintenance. Patch
  fixes land on them, and the docs are published from the head of the
  latest one.
- **Tags** (`vX.Y.Z`) mark the exact commit of each release, forever.
  GitHub Releases, `pip install git+…@vX.Y.Z`, and the changelog's
  version links point at tags.

Both are protected by repository rulesets. Only the release manager can
create or push release branches, and create release tags. Nobody can
move or delete a tag, and nobody can delete a release branch or
force-push to it. Commits on `main` and on release branches must be
signed, and `main` only changes through squash-merged pull requests.

Only the `flowli` package is published. `flowli-runner` and
`flowli-codeflow` stay in the repository.

### Publishing the documentation

The `Docs` workflow (`.github/workflows/docs.yml`) builds the site with
warnings as errors:

- on pull requests to `main` and release branches, as a check;
- on every push to `main`, so a merge that breaks the docs shows at once.
  `main` is never published;
- on pushes to `release/X.Y`, and then deploys to
  <https://quadratic-labs.github.io/flowli/> only when X.Y is the highest
  release branch on the remote. Backports to older release branches never
  overwrite the published site.

Build it locally with:

```bash
uv run --extra docs --extra api --extra cli make -C docs html
```

### A new minor release (X.Y.0)

1. On `main`, set the version in `pyproject.toml` and
   `src/flowli/__init__.py`. In `CHANGELOG.md`, move the `[Unreleased]`
   entries under a new `## [X.Y.0] - YYYY-MM-DD` heading, and add its link
   at the bottom:
   `[X.Y.0]: https://github.com/Quadratic-Labs/flowli/releases/tag/vX.Y.0`.
   Point `[Unreleased]` at `compare/vX.Y.0...HEAD`. Merge this through a
   pull request.
2. Cut the release branch from `main`. Pushing it publishes the docs:

   ```bash
   git switch main && git pull --ff-only
   git switch -c release/X.Y
   git push -u origin release/X.Y
   ```

3. Tag the release with a signed tag, then push the tag. The push
   publishes the release (see [Publishing a release](#publishing-a-release)):

   ```bash
   git tag -s vX.Y.0 -m "Flowli X.Y.0"
   git push origin vX.Y.0
   ```

### A patch release (X.Y.Z)

1. Land the fix on `main` first when it applies there, through a pull
   request as usual. A fix that only concerns the old line goes straight
   to the release branch; skip to step 2. Then bring the fix onto
   `release/X.Y` in one of two ways:

   - **Fast-forward**, when the release branch has no commits of its own
     since it was cut, and everything on `main` since then should ship:

     ```bash
     git fetch origin
     git log --oneline origin/release/X.Y..origin/main   # all of these will ship
     git switch release/X.Y && git merge --ff-only origin/main
     ```

   - **Cherry-pick** otherwise: when `main` has changes that must not ship
     in a patch, or when the release branch already has commits of its
     own. The version bump of any earlier patch release is such a commit,
     so from the second patch release on a line, this is the only option:

     ```bash
     git switch release/X.Y && git pull --ff-only
     git cherry-pick -x <commit-on-main>
     ```

   `git merge --ff-only` refuses to run when a fast-forward is not
   possible, so it is safe to try first.
2. On `release/X.Y`, bump the version to X.Y.Z in both files, add the
   changelog entry and its link, commit, and push. The docs republish if
   this is still the latest release branch.
3. Tag `vX.Y.Z` on the release branch with a signed tag, and push it.
   That publishes the release, as above.
4. Bring the changelog entry back to `main`, so `main`'s changelog lists
   every release.

### Publishing a release

Pushing a `vX.Y.Z` tag runs the `Publish` workflow
(`.github/workflows/publish.yml`):

1. **Build:** it fails unless the tag equals `v` plus the version in
   `pyproject.toml` and `flowli.__version__`, and unless the tagged commit
   is on `release/X.Y`. It then builds the sdist and wheel and checks them
   with `twine check --strict`.
2. **PyPI:** it uploads the distributions to
   [PyPI](https://pypi.org/project/flowli/) with Trusted Publishing,
   through the `pypi` deployment environment. No API token is stored
   anywhere: PyPI trusts this workflow in this repository.
3. **GitHub Release:** it creates the release for the tag, with the
   matching `CHANGELOG.md` section as notes and the distributions
   attached.

PyPI versions are immutable. A version can never be uploaded twice, even
after it is deleted, so check the version and the changelog before
pushing the tag. A mistake means a new patch version.

The trust between PyPI and this repository is configured on PyPI, in the
`flowli` project's publishing settings: owner `Quadratic-Labs`,
repository `flowli`, workflow `publish.yml`, environment `pypi`.

Build the distributions locally with:

```bash
uv build --no-sources            # sdist and wheel, into dist/
uvx twine check --strict dist/*
```
