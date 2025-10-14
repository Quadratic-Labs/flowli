Flowlet is a little workflow orchestration tool.

It is inspired by prefect, but here is what we don't need from it:

  1. Code deployment: this is done via standard CI/CD.
  1. Execution: we can launch azure jobs or whatever with API calls, no need to worry about workers.
  1. Cron

So what is left:

  1. Logging: execution traces are automatically logged to a database.
  1. Remote triggers: runs can be triggered by API calls.
  1. Admin dashboard.

What is extra:

  1. Access policies.

Optionally, also considering:

  1. Cross-cuttings: memoization and retries
