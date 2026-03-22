import json
import subprocess
import sys
import os
import ast
import threading
import time
from datetime import datetime, timezone

from celery import Celery
import redis
from pymongo import MongoClient

# Add backend directory to sys.path so we can import TaskState and ExecutionAttempt
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))
from models import TaskState, ExecutionAttempt

mongo_client = MongoClient("mongodb://localhost:27017")
db = mongo_client.tasks_db

app = Celery('tasks',
             broker='redis://localhost:6340/1',
             backend='redis://localhost:6340/2')

redis_list_client = redis.Redis(host='localhost', port=6340, db=0)


def validate_operator(operator_path: str) -> bool:
    """Validate that the script contains a class inheriting from BaseOperator and having initialize, run, finish."""
    try:
        if not os.path.exists(operator_path):
            return False
            
        with open(operator_path, 'r', encoding='utf-8') as f:
            tree = ast.parse(f.read())
            
        classes = [node for node in tree.body if isinstance(node, ast.ClassDef)]
        
        for cls in classes:
            bases = [b.id for b in cls.bases if isinstance(b, ast.Name) and hasattr(b, 'id')]
            if 'BaseOperator' not in bases:
                continue
                
            methods = [m.name for m in cls.body if isinstance(m, ast.FunctionDef)]
            if 'initialize' in methods and 'run' in methods and 'finish' in methods:
                return True
                
        return False
    except Exception as e:
        print(f"[Worker] Failed to validate operator script at {operator_path}: {e}")
        return False


@app.task
def check_redis_queue():
    print("[Celery Beat]: Checking Redis 'batch_run'...")
    
    task_done=0
    while True:
        task_name_bytes = redis_list_client.rpop("batch_run")
        if not task_name_bytes:
            break

        task_name = task_name_bytes.decode('utf-8')
        print(f"[Celery Beat]: Found task! '{task_name}'. Sending to worker...")

        execute_operator_task.delay(task_name)
        # print_the_name.delay(task_name)        
        task_done+=1

    if task_done>0:
        print(f"[Celery beat] : no. of invoked task = {task_done} ")
    else:
        print("[Celery Beat]: Batch_run is empty...No tasks found.")

@app.task
def print_the_name(task_name):
    print("-----------------------------------")
    print(f"[Celery Worker]: EXECUTING TASK: {task_name}")
    print("-----------------------------------")
    delete_task_from_queue_table_and_schedules_table.delay(task_name)
    return f"Task {task_name} processed."



@app.task
def mark_task_completed(task_name):
    """Mark a task as COMPLETED in both queue_table and schedules after successful execution."""
    db.queue_table.update_one({"task_name": task_name}, {"$set": {"state": TaskState.COMPLETED.value}})
    db["schedules"].update_one({"task_name": task_name}, {"$set": {"state": TaskState.COMPLETED.value}})
    print(f"marked task as completed - {task_name}")

@app.task
def mark_task_state(task_name: str, state: str, fail_reason: str = None):
    """Updates the state of a task in the database."""
    update_data = {"state": state}
    db.queue_table.update_one({"task_name": task_name}, {"$set": update_data})
    db["schedules"].update_one({"task_name": task_name}, {"$set": update_data})
    print(f"[Worker] Updated state for '{task_name}' to {state}")



def get_task_config(task_name: str):
    """Fetch a task document from MongoDB and extract its config.

    Args:
        task_name: Name of the task to look up in queue_table.

    Returns:
        Tuple of (task_doc, config) if found and valid, or (None, None) otherwise.
    """
    task_doc = db.queue_table.find_one({"task_name": task_name})
    if not task_doc:
        print(f"[Worker] '{task_name}' not found in queue_table. Skipping.")
        return None, None

    config = task_doc.get("task_config", {})
    if not config.get("operator_path"):
        print(f"[Worker] No operator_path in task_config for '{task_name}'. Skipping.")
        return None, None

    return task_doc, config


def spawn_operator_process(task_name: str, config: dict):
    """Spawn an operator script as a subprocess.

    Args:
        task_name: Name of the task (used for logging).
        config: Task config dict containing operator_path, payload, and connection.

    Returns:
        subprocess.Popen instance for the spawned operator process.
    """
    operator_path = config["operator_path"]
    payload = config.get("payload", {})
    connection = config.get("connection", {})

    process = subprocess.Popen(
        [sys.executable, operator_path,
         json.dumps(payload),
         json.dumps(connection)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    print(f"[Worker] '{task_name}' started as PID: {process.pid}")
    return process


def run_watchdog(task_name: str, process, timeout_seconds: int,
                 cancelled: threading.Event, timed_out: threading.Event,
                 run_done: threading.Event):
    """Watch for cancel signals (Redis) or timeout, and terminate the process if needed.

    Runs in a daemon thread alongside the operator subprocess. Polls Redis for
    a cancel:{task_name} key and checks the monotonic clock against the deadline.

    Args:
        task_name: Name of the task being watched.
        process: subprocess.Popen instance to terminate if needed.
        timeout_seconds: Maximum allowed runtime in seconds.
        cancelled: Event to set if a cancel signal is detected.
        timed_out: Event to set if the timeout is exceeded.
        run_done: Event that signals the main thread has finished waiting.
    """
    deadline = time.monotonic() + timeout_seconds
    while not run_done.is_set():
        if redis_list_client.exists(f"cancel:{task_name}"):
            print(f"[Watchdog] Cancel for '{task_name}'. Terminating.")
            redis_list_client.delete(f"cancel:{task_name}")
            cancelled.set()
            process.terminate()
            return

        if time.monotonic() > deadline:
            print(f"[Watchdog] Timeout for '{task_name}'. Terminating.")
            timed_out.set()
            process.terminate()
            return

        time.sleep(1)


def handle_process_result(task_name: str, exit_code: int,
                          cancelled: threading.Event, timed_out: threading.Event,
                          stdout: str, stderr: str, start_time: datetime, attempt_num: int = 1):
    """Handle post-process cleanup based on exit code and watchdog signals.

    Args:
        task_name: Name of the completed task.
        exit_code: Process return code.
        cancelled: Event indicating whether the task was cancelled.
        timed_out: Event indicating whether the task timed out.
        stdout: Captured stdout from the subprocess.
        stderr: Captured stderr from the subprocess.
        start_time: Process start time
        attempt_num: execution attempt number

    Returns:
        "success" if exit code is 0, "failed" if cancelled/timed out, "retry" otherwise.
    """
    if stdout: print(f"[Worker] STDOUT:\n{stdout}")
    if stderr: print(f"[Worker] STDERR:\n{stderr}")
    print(f"[Worker] '{task_name}' exited with code {exit_code}")

    end_time = datetime.now(timezone.utc)
    fail_reason = None
    state_to_set = None

    if cancelled.is_set():
        state_to_set = TaskState.CANCELLED.value
        fail_reason = "Task was cancelled."
        result = "failed"
    elif timed_out.is_set():
        state_to_set = TaskState.TIMED_OUT.value
        fail_reason = "Task timed out."
        result = "failed"
    elif exit_code == 0:
        state_to_set = TaskState.COMPLETED.value
        result = "success"
    else:
        state_to_set = TaskState.FAILED.value
        fail_reason = f"Exited with non-zero code {exit_code}\nSTDERR: {stderr}"
        result = "retry"

    execution_attempt = ExecutionAttempt(
        attempt_number=attempt_num,
        started_at=start_time,
        ended_at=end_time,
        state=state_to_set if result != "retry" else TaskState.FAILED.value,
        fail_reason=fail_reason
    ).model_dump(mode="json")
    
    # Push execution attempt to history
    update_query = {
        "$push": {"execution_history": execution_attempt}
    }
    
    # We update the state in DB depending on result
    if result == "success":
        update_query["$set"] = {
            "state": TaskState.COMPLETED.value,
            "completed_at": end_time.isoformat()
        }
    elif result == "failed":
        update_query["$set"] = {
            "state": state_to_set, # CANCELLED or TIMED_OUT
            "cancelled_at": end_time.isoformat() if cancelled.is_set() else None
        }
    
    db.queue_table.update_one({"task_name": task_name}, update_query)
    db["schedules"].update_one({"task_name": task_name}, update_query)
    
    return result


@app.task(bind=True, max_retries=3)
def execute_operator_task(self, task_name: str):
    """Execute a task by spawning its operator script as a subprocess.

    Fetches task config from MongoDB, spawns the operator, starts a watchdog
    thread for cancel/timeout monitoring, and handles the result.

    Args:
        task_name: Name of the task to execute.
    """
    task_doc, config = get_task_config(task_name)
    if not config:
        mark_task_state(task_name, TaskState.INVALID.value)
        return

    operator_path = config.get("operator_path")
    if not operator_path or not validate_operator(operator_path):
        print(f"[Worker] Script validation failed for '{task_name}' at {operator_path}")
        mark_task_state(task_name, TaskState.INVALID.value)
        return
        
    mark_task_state(task_name, TaskState.RUNNING.value)

    timeout_seconds = config.get("timeout_seconds", 3600)
    process = spawn_operator_process(task_name, config)
    start_time = datetime.now(timezone.utc)
    attempt_num = self.request.retries + 1

    cancelled = threading.Event()
    timed_out = threading.Event()
    run_done = threading.Event()

    watchdog_thread = threading.Thread(
        target=run_watchdog,
        args=(task_name, process, timeout_seconds, cancelled, timed_out, run_done),
        daemon=True
    )
    watchdog_thread.start()

    stdout, stderr = process.communicate()
    run_done.set()
    watchdog_thread.join(timeout=5)

    exit_code = process.returncode
    result = handle_process_result(task_name, exit_code, cancelled, timed_out, stdout, stderr, start_time, attempt_num)
    
    if result == "retry":
        db.queue_table.update_one({"task_name": task_name}, {"$inc": {"num_of_retries": 1}})
        db["schedules"].update_one({"task_name": task_name}, {"$inc": {"num_of_retries": 1}})
        
        # Check max_retries
        max_retries = task_doc.get("max_retries", 3)
        if attempt_num >= max_retries:
            mark_task_state(task_name, TaskState.EXHAUSTED.value)
        else:
            mark_task_state(task_name, TaskState.RETRY.value)
            raise self.retry(countdown=30)


app.conf.beat_schedule = {
    'check-redis-every-5-seconds': {
        'task': 'tasks.check_redis_queue',
        'schedule': 5.0,
    },
}
app.conf.timezone = 'Asia/Kolkata'
