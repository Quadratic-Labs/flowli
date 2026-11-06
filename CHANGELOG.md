# Changelog

All notable changes to Flowli are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project
uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html). Before
1.0, minor versions may contain breaking changes. They are marked
**Breaking** below.

## [Unreleased]

### Added

- A Workflows table at the end of the dashboard: for each workflow, five
  dots for the statuses of its last five executions (newest on the right,
  grey for no run yet), the run and active counts, the success rate and the
  mean duration of the last 20 finished executions, the time since the last
  run, and a Start button. It reads the new `GET /stats/workflows`
  (`projection.workflow_summaries`), joined with the catalog so that a
  workflow that never ran still shows.

### Fixed

- The web application's execution list kept showing a finished execution
  as running. The worker stamped `execution.completed`, `failed`,
  `cancelled` and `suspended` with the instant the run *started*, so the
  projection's `updated_at` did not move. The ETag of `/executions` and
  `/executions/{eid}/children`, built from `eid@updated_at` only, then gave a
  304 on the stale list. Outcome entries now carry the instant the run ended
  (which also fixes the elapsed time and the newest-first order). The list
  ETags now cover every field of every row.

## [0.2.0] - 2026-11-02

### Added

- `--storage module:attr` (or `FLOWLI_STORAGE`) on every command: a
  CairnDB `StorageConfig` or `BlobStorage`, or a callable that returns one.
  An application builds its bucket from its own settings, credentials
  included, and the CLI still owns the backend (`--projection`,
  `--worker-id`). It excludes `--storage-path` and takes precedence over
  the `CAIRNDB_*` variables.

### Changed

- CairnDB `>=0.4.1,<0.6`, so that 0.5 and its Azure `credential` can be used.

## [0.1.1] - 2026-11-01

### Fixed

- `flowli sweeper --projection` and `flowli retention --projection` no
  longer fail with `unable to open database file` before the first
  execution: on a control log with no commit, the projection's sweeper and
  retention sources answer empty.

## [0.1.0] - 2026-09-26

First release, published to PyPI (`pip install flowli`). The project was
developed as Flowlet before this release.

### Added

- The engine: a workflow runs as a coroutine execution stack, and every
  frame is journaled to CairnDB. Replay from the journal resumes an
  execution, and leases give each execution exactly one owner.
- Patterns built on the `Context` API: delegate, fanout, review, saga
  and schedule.
- The `flowli` command (the `cli` extra): the worker, sweeper and
  retention jobs, and the operator commands `status`, `signal`, `cancel`
  and `migrate`.
- The HTTP service (the `api` extra): the catalog, the control plane,
  the worker plane and evidence.
- Backends: CairnDB (`cairndb>=0.4.1,<0.5`, from PyPI) and in-memory.
- A `py.typed` marker, and the MIT `LICENSE` in the sdist and wheel.
- Publishing: pushing a `vX.Y.Z` tag on `release/X.Y` publishes the
  release to PyPI (Trusted Publishing) and creates the GitHub Release.
- The documentation, published to GitHub Pages from the latest
  `release/X.Y` branch, with a Project section: roadmap, changelog,
  contributing guide and code of conduct.
- A CI workflow: `ruff check`, `ruff format --check`, `mypy` and the
  tests of the three packages.

[Unreleased]: https://github.com/Quadratic-Labs/flowli/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/Quadratic-Labs/flowli/releases/tag/v0.2.0
[0.1.1]: https://github.com/Quadratic-Labs/flowli/releases/tag/v0.1.1
[0.1.0]: https://github.com/Quadratic-Labs/flowli/releases/tag/v0.1.0
