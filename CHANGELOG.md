# Changelog

All notable changes to Flowli are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project
uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html). Before
1.0, minor versions may contain breaking changes. They are marked
**Breaking** below.

## [Unreleased]

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

[Unreleased]: https://github.com/Quadratic-Labs/flowli/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/Quadratic-Labs/flowli/releases/tag/v0.1.0
