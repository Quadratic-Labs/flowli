"""
Example flows demonstrating Flowlet features
"""
import time
import random
from datetime import datetime

from taskflow import Taskflow


# flowlet = configure({"database": {"url": "sqlite:///flowlet_example.db"}})
# flowlet.init_database()
flowlet = Taskflow.configure({
    "storage": {"type": "filesystem", "path": "./storage"},
    "queue": {"type": "memory"},
})


# region Tasks
# ============================================================================

@flowlet.task()
def fetch_data(source: str):
    """Simulate fetching data from a source"""
    print(f"Fetching data from {source}...")
    time.sleep(15)
    return {
        "source": source,
        "data": [1, 2, 3, 4, 5],
        "timestamp": datetime.now().isoformat()
    }


@flowlet.task()
def transform_data(data: dict):
    """Transform the data by doubling all values"""
    print(f"Transforming data from {data['source']}...")
    time.sleep(0.3)
    transformed = [x * 2 for x in data["data"]]
    return {
        **data,
        "data": transformed,
        "transformed": True
    }


@flowlet.task()
def validate_data(data: dict):
    """Validate the data meets requirements"""
    print("Validating data...")
    time.sleep(0.2)
    is_valid = all(x > 0 for x in data["data"])
    if not is_valid:
        raise ValueError("Data validation failed: found non-positive values")
    return {**data, "validated": True}


@flowlet.task()
def save_to_database(data: dict, table_name: str = "results"):
    """Simulate saving data to a database"""
    print(f"Saving data to table '{table_name}'...")
    time.sleep(0.4)
    return {
        "table": table_name,
        "records_saved": len(data["data"]),
        "timestamp": datetime.now().isoformat()
    }


@flowlet.task()
def send_notification(message: str, recipient: str):
    """Simulate sending a notification"""
    print(f"Sending notification to {recipient}: {message}")
    time.sleep(0.2)
    return {"sent": True, "recipient": recipient}


@flowlet.task()
def calculate_statistics(numbers: list):
    """Calculate basic statistics on a list of numbers"""
    print("Calculating statistics...")
    time.sleep(0.3)
    return {
        "count": len(numbers),
        "sum": sum(numbers),
        "avg": sum(numbers) / len(numbers) if numbers else 0,
        "min": min(numbers) if numbers else None,
        "max": max(numbers) if numbers else None,
    }


@flowlet.task()
def risky_operation(fail_probability: float = 0.3):
    """A task that randomly fails to demonstrate error handling"""
    print(f"Running risky operation (fail probability: {fail_probability})...")
    time.sleep(0.5)
    if random.random() < fail_probability:
        raise RuntimeError("Random failure occurred in risky operation!")
    return {"success": True, "lucky": True}

# ============================================================================
# endregion

# region Flows
# ============================================================================

@flowlet.flow("simple_etl", timeout=120, max_retries=3)
def simple_etl_flow(source: str = "api"):
    """
    A simple ETL (Extract, Transform, Load) flow that:
    1. Fetches data from a source
    2. Transforms the data
    3. Validates it
    4. Saves it to a database
    """
    # Extract
    raw_data = fetch_data(source)

    # Transform
    transformed = transform_data(raw_data)

    # Validate
    validated = validate_data(transformed)

    # Load
    result = save_to_database(validated, table_name="processed_data")

    return {
        "status": "completed",
        "source": source,
        "records_processed": result["records_saved"]
    }


@flowlet.flow("data_pipeline")
def data_pipeline_flow(source: str = "database", notify: bool = True):
    """
    A more complex data pipeline that:
    1. Fetches and transforms data
    2. Calculates statistics
    3. Saves results
    4. Optionally sends notifications
    """
    # Fetch and transform
    raw_data = fetch_data(source)
    transformed = transform_data(raw_data)

    # Calculate statistics on transformed data
    stats = calculate_statistics(transformed["data"])

    # Save to database
    save_result = save_to_database(transformed, table_name="analytics")

    # Optional notification
    if notify:
        notification = send_notification(
            message=f"Pipeline completed: {stats['count']} records processed",
            recipient="admin@example.com"
        )
    else:
        notification = None

    return {
        "status": "success",
        "statistics": stats,
        "saved": save_result,
        "notification_sent": notification is not None
    }


@flowlet.flow("hello_world")
def hello_world_flow(name: str = "World"):
    """
    A simple hello world flow to demonstrate basic functionality
    """
    message = f"Hello, {name}!"
    print(message)
    return {"message": message, "timestamp": datetime.now().isoformat()}


@flowlet.flow("parallel_tasks")
def parallel_tasks_flow(count: int = 3):
    """
    Demonstrates running multiple independent tasks
    (Note: tasks run sequentially in this example, but could be parallelized)
    """
    results = []

    for i in range(count):
        data = fetch_data(f"source_{i}")
        stats = calculate_statistics(data["data"])
        results.append(stats)

    return {
        "task_count": count,
        "results": results
    }


@flowlet.flow("error_handling_demo", timeout=60, max_retries=2)
def error_handling_demo_flow(fail_chance: float = 0.5):
    """
    Demonstrates error handling by running a risky operation
    """
    # try:
    result = risky_operation(fail_probability=fail_chance)
    send_notification(
        message="Risky operation succeeded!",
        recipient="success@example.com"
    )
    return {"status": "success", "result": result}
    # except Exception as e:
    #     # Even though we catch the exception, the task will be marked as failed
    #     # This demonstrates that you can handle errors in your flow logic
    #     send_notification(
    #         message=f"Risky operation failed: {str(e)}",
    #         recipient="alerts@example.com"
    #     )
    #     return {"status": "handled_error", "error": str(e)}

# ============================================================================
# endregion
