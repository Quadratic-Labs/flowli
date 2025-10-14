import time

from flowlet.database import get_db_session_factory
from flowlet.manager import FlowManager

# ---------------------------
# Example: user flows and tasks
# ---------------------------
# These are example tasks and flows. Replace or add your own flows by using
# the @flow and @task decorators.

fm = FlowManager(get_db_session_factory())


@fm.task()
def add(a, b):
    time.sleep(0.2)
    return a + b


@fm.task()
def mul(a, b):
    time.sleep(0.3)
    return a * b


@fm.task()
def maybe_fail(x):
    time.sleep(0.1)
    if x == 13:
        raise ValueError("unlucky number!")
    return x


@fm.flow("simple_math")
def simple_math_flow(x: int, y: int):
    s = add(x, y)
    p = mul(x, y)
    r = maybe_fail(x)
    return {"sum": s, "prod": p, "passed": r}