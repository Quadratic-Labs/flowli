# Changelog

All notable changes to Flowli are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project
uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html). Before
1.0, minor versions may contain breaking changes. They are marked
**Breaking** below.

## [Unreleased]

### Added

- `LICENSE` file with the MIT license text, shipped in the sdist and
  wheel.
- Publishing: pushing a `vX.Y.Z` tag on `release/X.Y` publishes the
  release to PyPI (Trusted Publishing) and creates the GitHub Release.
- The documentation is built on every push to `main`, and published to
  GitHub Pages from the latest `release/X.Y` branch.
- A CI workflow: `ruff check`, `ruff format --check`, `mypy` and the
  tests of the three packages, on every pull request and on pushes to
  `main` and the release branches.
- A Project section in the documentation: roadmap, changelog,
  contributing guide (with the release process), and code of conduct.

### Fixed

- The `execution.created` entry on the control log has the frame id
  `root`, like every other `execution.*` entry, instead of none.

### Changed

- **Breaking:** the project is renamed from Flowlet to Flowli: the
  `flowli` package and command, the `flowli-runner` and
  `flowli-codeflow` packages, and the `FLOWLI_*` environment variables.
- CairnDB is installed from PyPI (`cairndb>=0.4.1,<0.5`).
- `uvicorn` moves from the core dependencies to the `api` and `cli`
  extras.
- The specifications move from `docs/specs/` to `specs/`, and are no
  longer part of the documentation site.
- The code is formatted with `ruff format`.

[Unreleased]: https://github.com/Quadratic-Labs/flowli/commits/main
