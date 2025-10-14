"""
Tests demonstrating thread and async safety of contextvar-based execution tracking.

These tests prove that ContextVars provide proper isolation between:
- Multiple threads running flows concurrently
- Multiple async tasks running flows concurrently
- Nested contexts (flows calling flows)
"""

import asyncio
import contextvars
import threading
import time
from typing import List


# Simulating the context variables from your system
current_flow_run_id: contextvars.ContextVar[str] = contextvars.ContextVar(
    'current_flow_run_id', default=None
)


# ============================================================================
# Thread Safety Tests
# ============================================================================

def test_thread_safety_basic():
    """Test that each thread sees its own flow_run_id."""
    results = {}

    def run_flow(flow_id: str):
        # Set context for this thread
        token = current_flow_run_id.set(flow_id)

        try:
            # Simulate work
            time.sleep(0.01)

            # Record what this thread sees
            results[flow_id] = current_flow_run_id.get()

        finally:
            current_flow_run_id.reset(token)

    # Run multiple flows in parallel
    threads = []
    for i in range(5):
        flow_id = f"flow-{i}"
        t = threading.Thread(target=run_flow, args=(flow_id,))
        threads.append(t)
        t.start()

    # Wait for all threads
    for t in threads:
        t.join()

    # Verify each thread saw its own ID (no interference)
    assert len(results) == 5
    for i in range(5):
        flow_id = f"flow-{i}"
        assert results[flow_id] == flow_id, \
            f"Thread {i} saw wrong flow_id: {results[flow_id]}"

    print("✅ Thread safety test passed - no interference between threads")


def test_thread_safety_with_tasks():
    """Test flow-task relationship across threads."""
    results = []

    def run_task(task_name: str):
        """Simulate a task that reads flow_run_id from context."""
        flow_run_id = current_flow_run_id.get()
        results.append({
            'task': task_name,
            'flow_run_id': flow_run_id
        })

    def run_flow(flow_id: str):
        """Simulate a flow that spawns tasks."""
        token = current_flow_run_id.set(flow_id)

        try:
            # Run multiple tasks in this flow
            run_task(f"{flow_id}-task1")
            run_task(f"{flow_id}-task2")

        finally:
            current_flow_run_id.reset(token)

    # Run two flows in parallel
    t1 = threading.Thread(target=run_flow, args=("flow-A",))
    t2 = threading.Thread(target=run_flow, args=("flow-B",))

    t1.start()
    t2.start()
    t1.join()
    t2.join()

    # Verify tasks saw correct flow IDs
    assert len(results) == 4

    flow_a_tasks = [r for r in results if r['flow_run_id'] == 'flow-A']
    flow_b_tasks = [r for r in results if r['flow_run_id'] == 'flow-B']

    assert len(flow_a_tasks) == 2
    assert len(flow_b_tasks) == 2

    print("✅ Flow-task relationship preserved across threads")


# ============================================================================
# Async Safety Tests
# ============================================================================

async def test_async_safety_basic():
    """Test that each async task sees its own flow_run_id."""
    results = {}

    async def run_flow(flow_id: str):
        # Set context for this async task
        token = current_flow_run_id.set(flow_id)

        try:
            # Simulate async work
            await asyncio.sleep(0.01)

            # Record what this task sees
            results[flow_id] = current_flow_run_id.get()

        finally:
            current_flow_run_id.reset(token)

    # Run multiple flows concurrently
    await asyncio.gather(*[
        run_flow(f"async-flow-{i}")
        for i in range(5)
    ])

    # Verify each task saw its own ID
    assert len(results) == 5
    for i in range(5):
        flow_id = f"async-flow-{i}"
        assert results[flow_id] == flow_id

    print("✅ Async safety test passed - no interference between async tasks")


async def test_async_context_preservation():
    """Test that context is preserved across await boundaries."""
    async def run_task():
        flow_id_before = current_flow_run_id.get()
        await asyncio.sleep(0.01)
        flow_id_after = current_flow_run_id.get()
        return flow_id_before, flow_id_after

    async def run_flow(flow_id: str):
        token = current_flow_run_id.set(flow_id)
        try:
            before, after = await run_task()
            return before, after
        finally:
            current_flow_run_id.reset(token)

    before, after = await run_flow("test-flow")

    # Context should be preserved across await
    assert before == "test-flow"
    assert after == "test-flow"

    print("✅ Context preserved across await boundaries")


# ============================================================================
# Nested Context Tests
# ============================================================================

def test_nested_contexts():
    """Test that nested contexts work correctly with tokens."""
    outer_id = "outer-flow"
    inner_id = "inner-flow"

    outer_token = current_flow_run_id.set(outer_id)

    try:
        # Verify outer context
        assert current_flow_run_id.get() == outer_id

        # Set inner context
        inner_token = current_flow_run_id.set(inner_id)

        try:
            # Verify inner context
            assert current_flow_run_id.get() == inner_id

        finally:
            # Reset inner context
            current_flow_run_id.reset(inner_token)

        # Verify outer context restored
        assert current_flow_run_id.get() == outer_id

    finally:
        # Reset outer context
        current_flow_run_id.reset(outer_token)

    # Verify context fully cleared
    assert current_flow_run_id.get() is None

    print("✅ Nested contexts work correctly with tokens")


# ============================================================================
# Comparison: ContextVar vs Global Variable (to show the problem)
# ============================================================================

# Global variable (NOT thread-safe)
global_flow_id = None


def test_global_variable_race_condition():
    """Demonstrate why global variables are NOT thread-safe."""
    results = {}

    def run_flow_with_global(flow_id: str):
        global global_flow_id
        global_flow_id = flow_id  # Race condition!

        time.sleep(0.01)  # Simulate work

        # This might see a different flow_id!
        results[flow_id] = global_flow_id

    threads = []
    for i in range(3):
        flow_id = f"global-flow-{i}"
        t = threading.Thread(target=run_flow_with_global, args=(flow_id,))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    # Check if any thread saw the wrong ID (race condition)
    errors = []
    for i in range(3):
        flow_id = f"global-flow-{i}"
        if flow_id in results and results[flow_id] != flow_id:
            errors.append(f"Thread {i} saw {results[flow_id]} instead of {flow_id}")

    if errors:
        print(f"❌ Global variable has race conditions: {errors}")
    else:
        print("⚠️  Global variable might appear to work, but is NOT guaranteed safe")


# ============================================================================
# Main Test Runner
# ============================================================================

def run_all_tests():
    """Run all context safety tests."""
    print("\n" + "="*70)
    print("Context Safety Tests - ContextVars vs Global Variables")
    print("="*70 + "\n")

    print("1. Testing thread safety with ContextVars...")
    test_thread_safety_basic()

    print("\n2. Testing flow-task relationship across threads...")
    test_thread_safety_with_tasks()

    print("\n3. Testing nested contexts...")
    test_nested_contexts()

    print("\n4. Testing async safety with ContextVars...")
    asyncio.run(test_async_safety_basic())

    print("\n5. Testing context preservation across await...")
    asyncio.run(test_async_context_preservation())

    print("\n6. Demonstrating global variable race conditions...")
    test_global_variable_race_condition()

    print("\n" + "="*70)
    print("Summary: ContextVars provide safe, isolated context in both")
    print("threaded and async environments. Global variables do NOT.")
    print("="*70 + "\n")


if __name__ == "__main__":
    run_all_tests()
