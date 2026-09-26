# Roadmap

Flowli is **alpha** software. The engine, the patterns, the CLI and the
HTTP service are implemented and tested. Now they need to prove
themselves in real use. This page describes where the project is and
what comes next. It states intent, not commitments: priorities shift with
what we learn.

## Current phase: testing and validation

The API is not frozen yet. Before 1.0, we want evidence that the model
(a journaled coroutine execution stack, replayed from the top) is correct,
sufficient, and pleasant to build on.

### Finding bugs

- Keep the test suite exhaustive: the in-memory backend for the rules,
  CairnDB on the filesystem backend for real contention, and mutation
  testing.
- Hunt for the bugs that tests miss: long-running executions, large
  journals, many concurrent workers, and crashes at every point of the
  worker loop.
- Report anything surprising. See [Contributing](contributing.md).

### Building applications on top of Flowli

The best test of an API is building real things with it.
`flowli-runner` and `flowli-codeflow`, which run coding agents as
workflows, are the first applications. They answer two questions:

- **Is the API useful?** Do `Context`, the patterns and the operator
  commands express real workflows naturally?
- **Are primitives missing?** Patterns that recur across applications are
  candidates for new patterns, or for new `Context` verbs.

The API may change as a result, with breaking changes recorded in the
[Changelog](changelog.md).

## Next

- The first release on PyPI.
- Validation on the cloud storage backends of CairnDB, not only on the
  filesystem backend.

## Later

- Publishing `flowli-runner` and `flowli-codeflow` once their API
  settles.
- A 1.0 release with a stable API, once the validation phase has settled
  the model.
