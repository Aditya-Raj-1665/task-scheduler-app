'''
Test functions for task management operations.

How to run:
    1. Ensure MongoDB and Redis are running locally :
        docker-compose up -d
    3. pip install --upgrade pydantic pydantic-core
    2. Run `pytest` in the worker/ directory to execute these tests. 
           pytest worker/tests/test_tasks.py -v -s -p no:warnings
'''

import sys
import os
import time
import pytest
from pymongo import MongoClient
import redis
import threading

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "backend"))
from tasks import (
    app,
    get_task_config,
    spawn_operator_process,
    run_watchdog,
    handle_process_result,
    check_redis_queue,
)
from models import TaskState
app.conf.task_always_eager = True

@pytest.fixture
def mongo_db():
    """Connect to real MongoDB, yield the db, then clean up all test_ docs."""
    client = MongoClient("mongodb://localhost:27017")
    db = client.tasks_db
    yield db
    # Cleanup: delete anything inserted by tests
    db.queue_table.delete_many({"task_name": {"$regex": "^test_"}})
    db["schedules"].delete_many({"task_name": {"$regex": "^test_"}})
    client.close()


@pytest.fixture
def redis_client():
    """Connect to real Redis, yield client, then clean up all test_ keys."""
    client = redis.Redis(host="localhost", port=6340, db=0)
    yield client
    # Cleanup: delete any keys starting with test_ or cancel:test_
    for key in client.keys("test_*"):
        client.delete(key)
    for key in client.keys("cancel:test_*"):
        client.delete(key)
  

#--HELPER FUNCTIONS--  
def insert_task(mongo_db, task_name, task_config=None, status="pending"):
    """Insert a task document into queue_table with given config."""
    mongo_db.queue_table.insert_one({
        "task_name": task_name,
        "status": status,
        "task_config": task_config or {}
    })


def insert_schedule(mongo_db, task_name):
    """Insert a schedule document for a task."""
    mongo_db["schedules"].insert_one({"task_name": task_name})



class TestGetTaskConfig:
    def test_valid_task_returns_doc_and_config(self, mongo_db):
        """task with operator_path, should return real doc and config."""
        task_config = {
            "operator_path": "/worker/operators/my_operator.py",
            "payload": {"key": "value"},
            "connection": {"host": "localhost"},
            "timeout_seconds": 120
        }
        insert_task(mongo_db, "test_valid_task", task_config=task_config)

        doc, config = get_task_config("test_valid_task")

        assert doc is not None
        assert doc["task_name"] == "test_valid_task"
        assert config is not None
        assert config["operator_path"] == "/worker/operators/my_operator.py"
        assert config["payload"] == {"key": "value"}
        assert config["timeout_seconds"] == 120



class TestSpawnOperatorProcess:
    def test_spawn_runs_real_python_script(self, tmp_path):
        """Spawn a real Python script and verify it exits with code 0."""
        # Create a tiny throwaway script
        script = tmp_path / "op.py"
        script.write_text("print('hello from operator')")

        config = {
            "operator_path": str(script),
            "payload": {},
            "connection": {}
        }

        process = spawn_operator_process("test_spawn_task", config)
        stdout, stderr = process.communicate()

        assert process.returncode == 0
        assert "hello from operator" in stdout

    def test_spawn_passes_payload_as_argument(self, tmp_path):
        """Verify payload JSON is passed correctly to the operator script."""
        script = tmp_path / "op.py"
        # Script prints sys.argv[1] (the payload argument)
        script.write_text("import sys; print(sys.argv[1])")

        config = {
            "operator_path": str(script),
            "payload": {"foot": "ball"},
            "connection": {}
        }

        process = spawn_operator_process("test_payload_task", config)
        stdout, stderr = process.communicate()

        assert '"foot": "ball"' in stdout

    def test_spawn_invalid_script_path_fails(self, tmp_path):
        """Spawning a non-existent script should result in non-zero exit code."""
        config = {
            "operator_path": "/totally/fake/path/operator.py",
            "payload": {},
            "connection": {}
        }

        process = spawn_operator_process("test_bad_path_task", config)
        stdout, stderr = process.communicate()

        assert process.returncode != 0

    def test_spawn_script_that_raises_exception(self, tmp_path):
        """Operator script that crashes should give non-zero exit code."""
        script = tmp_path / "op.py"
        script.write_text("raise ValueError('something went wrong')")

        config = {
            "operator_path": str(script),
            "payload": {},
            "connection": {}
        }

        process = spawn_operator_process("test_crash_task", config)
        stdout, stderr = process.communicate()

        assert process.returncode != 0
        assert "ValueError" in stderr


# ─────────────────────────────────────────────────────────────────────────────
# SECTION : run_watchdog
# Tests the watchdog thread that monitors cancel signal and timeout
# ─────────────────────────────────────────────────────────────────────────────

class TestRunWatchdog:
    """
    Watchdog runs in a thread alongside a real subprocess.
    We test cancel via Redis key and timeout via a short timeout value.
    """

    def test_watchdog_cancels_on_redis_signal(self, tmp_path, redis_client):
        """If cancel:<task_name> key appears in Redis, watchdog should terminate process."""

        # Long-running script (sleeps 60s) — watchdog should kill it
        script = tmp_path / "op.py"
        script.write_text("import time; time.sleep(60)")

        import subprocess, sys
        process = subprocess.Popen(
            [sys.executable, str(script)],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )

        cancelled = threading.Event()
        timed_out = threading.Event()
        run_done = threading.Event()

        thread = threading.Thread(
            target=run_watchdog,
            args=("test_cancel_task", process, 60, cancelled, timed_out, run_done),
            daemon=True
        )
        thread.start()

        # Simulate cancel signal via Redis after a short delay
        time.sleep(1)
        redis_client.set("cancel:test_cancel_task", "1")

        thread.join(timeout=10)
        process.wait()

        assert cancelled.is_set()
        assert not timed_out.is_set()

    def test_watchdog_times_out(self, tmp_path):
        """If process runs longer than timeout_seconds, watchdog should terminate it."""
        import threading, subprocess, sys

        script = tmp_path / "op.py"
        script.write_text("import time; time.sleep(60)")

        process = subprocess.Popen(
            [sys.executable, str(script)],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )

        cancelled = threading.Event()
        timed_out = threading.Event()
        run_done = threading.Event()

        # Very short timeout to trigger quickly
        thread = threading.Thread(
            target=run_watchdog,
            args=("test_timeout_task", process, 2, cancelled, timed_out, run_done),
            daemon=True
        )
        thread.start()
        thread.join(timeout=10)
        process.wait()

        assert timed_out.is_set()
        assert not cancelled.is_set()

    def test_watchdog_exits_cleanly_when_process_finishes(self, tmp_path):
        """If process finishes normally and run_done is set, watchdog should exit without killing."""
        import threading, subprocess, sys

        script = tmp_path / "op.py"
        script.write_text("print('done')")

        process = subprocess.Popen(
            [sys.executable, str(script)],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )

        cancelled = threading.Event()
        timed_out = threading.Event()
        run_done = threading.Event()

        thread = threading.Thread(
            target=run_watchdog,
            args=("test_clean_exit_task", process, 30, cancelled, timed_out, run_done),
            daemon=True
        )
        thread.start()

        process.wait()
        run_done.set()  # Signal watchdog that process is done
        thread.join(timeout=5)

        # Neither cancel nor timeout should have fired
        assert not cancelled.is_set()
        assert not timed_out.is_set()


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 4: handle_process_result
# Tests the helper that decides what to do after process exits
# ─────────────────────────────────────────────────────────────────────────────

class TestHandleProcessResult:
    """
    handle_process_result returns a status string:
      "success"  → task deleted from DB
      "failed"   → task marked FAILED in DB
      "retry"    → caller should retry
    """

    def test_exit_code_0_returns_success(self, mongo_db):
        """Process succeeded (exit 0) → should return 'success'."""
        import threading
        insert_task(mongo_db, "test_result_success")

        cancelled = threading.Event()
        timed_out = threading.Event()

        result = handle_process_result(
            "test_result_success", exit_code=0,
            cancelled=cancelled, timed_out=timed_out,
            stdout="some output", stderr=""
        )

        assert result == "success"

    def test_exit_code_0_deletes_task_from_db(self, mongo_db):
        """On success, task should be removed from queue_table."""
        import threading
        insert_task(mongo_db, "test_delete_on_success")
        insert_schedule(mongo_db, "test_delete_on_success")

        cancelled = threading.Event()
        timed_out = threading.Event()

        handle_process_result(
            "test_delete_on_success", exit_code=0,
            cancelled=cancelled, timed_out=timed_out,
            stdout="", stderr=""
        )

        # Give the .delay() call a moment to execute (it's async)
        time.sleep(1)

        doc = mongo_db.queue_table.find_one({"task_name": "test_delete_on_success"})
        assert doc is None

    def test_cancelled_returns_failed(self, mongo_db):
        """If watchdog cancelled the process → should return 'failed'."""
        import threading
        insert_task(mongo_db, "test_result_cancelled")

        cancelled = threading.Event()
        cancelled.set()  # Simulate cancel
        timed_out = threading.Event()

        result = handle_process_result(
            "test_result_cancelled", exit_code=1,
            cancelled=cancelled, timed_out=timed_out,
            stdout="", stderr=""
        )

        assert result == "failed"

    def test_cancelled_marks_task_failed_in_db(self, mongo_db):
        """Cancelled task should have status=FAILED in MongoDB."""
        import threading
        insert_task(mongo_db, "test_db_failed_cancel")

        cancelled = threading.Event()
        cancelled.set()
        timed_out = threading.Event()

        handle_process_result(
            "test_db_failed_cancel", exit_code=1,
            cancelled=cancelled, timed_out=timed_out,
            stdout="", stderr=""
        )

        doc = mongo_db.queue_table.find_one({"task_name": "test_db_failed_cancel"})
        assert doc["status"] == "FAILED"

    def test_timed_out_returns_failed(self, mongo_db):
        """If watchdog timed out the process → should return 'failed'."""
        import threading
        insert_task(mongo_db, "test_result_timeout")

        cancelled = threading.Event()
        timed_out = threading.Event()
        timed_out.set()  # Simulate timeout

        result = handle_process_result(
            "test_result_timeout", exit_code=1,
            cancelled=cancelled, timed_out=timed_out,
            stdout="", stderr=""
        )

        assert result == "failed"

    def test_non_zero_exit_no_cancel_no_timeout_returns_retry(self, mongo_db):
        """Process failed on its own (not cancelled/timeout) → should return 'retry'."""
        import threading
        insert_task(mongo_db, "test_result_retry")

        cancelled = threading.Event()
        timed_out = threading.Event()

        result = handle_process_result(
            "test_result_retry", exit_code=1,
            cancelled=cancelled, timed_out=timed_out,
            stdout="", stderr="some error", start_time=datetime.now(timezone.utc), attempt_num=1
        )

        assert result == "retry"

