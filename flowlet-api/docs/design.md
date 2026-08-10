Design
======

The user's flow:

1. User builds the flowlet object with its configurations.
2. User decorates functions to convert them into tasks and flows. This involves the FlowRegistry, but also a FlowExecutor as it needs to be able to call other flows and tasks naturally as a usual function.
3. User calls a flow via python or RPC.

The execution's flow:

1. A context is initialised.
2. The tracer generates logs during execution.
3. The exporter writes them to a storage.
4. Subflows or tasks get their own derived context.