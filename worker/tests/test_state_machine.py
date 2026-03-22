import sys
import os
import time
import pytest
from pymongo import MongoClient
import redis
import threading

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'backend'))

from models import TaskState
from .test_tasks import mongo_db, redis_client, insert_task

class TestStateMachine:
    def test_state_transitions_success(self, mongo_db, tmp_path):
        from tasks import execute_operator_task, db
        import json
        
        script = tmp_path / "dummy_op.py"
        script.write_text("import sys\nimport os\nsys.path.append(os.path.abspath('worker'))\nfrom base_operator import BaseOperator, run_operator\nclass DummyOp(BaseOperator):\n    def initialize(self, p, c): pass\n    def run(self): pass\n    def finish(self): pass\nif __name__ == '__main__':\n    run_operator(DummyOp(), {}, {})\n")

        insert_task(mongo_db, "test_state_success", {
            "operator_path": str(script),
            "payload": {},
            "connection": {}
        }, status="PENDING")
        mongo_db["schedules"].insert_one({"task_name": "test_state_success", "state": "PENDING", "max_retries": 3})
        
        try:
            execute_operator_task.apply(args=("test_state_success",))
        except Exception:
            pass
        
        task = mongo_db.queue_table.find_one({"task_name": "test_state_success"})
        assert task["state"] == TaskState.COMPLETED.value
        assert len(task.get("execution_history", [])) == 1

    def test_state_invalid_operator(self, mongo_db, tmp_path):
        from tasks import execute_operator_task
        script = tmp_path / "bad_op.py"
        script.write_text("print('no base operator')")
        
        insert_task(mongo_db, "test_state_invalid", {
            "operator_path": str(script),
            "payload": {},
            "connection": {}
        }, status="PENDING")
        mongo_db["schedules"].insert_one({"task_name": "test_state_invalid", "state": "PENDING"})
        
        execute_operator_task.apply(args=("test_state_invalid",))
        task = mongo_db.queue_table.find_one({"task_name": "test_state_invalid"})
        assert task["state"] == TaskState.INVALID.value

    def test_state_cancelled(self, mongo_db, tmp_path, redis_client):
        from tasks import execute_operator_task
        import threading, time
        
        script = tmp_path / "cancel_op.py"
        script.write_text("import time\nimport sys\nimport os\nsys.path.append(os.path.abspath('worker'))\nfrom base_operator import BaseOperator, run_operator\nclass DummyOp(BaseOperator):\n    def initialize(self, p, c): pass\n    def run(self): time.sleep(10)\n    def finish(self): pass\nif __name__ == '__main__':\n    run_operator(DummyOp(), {}, {})\n")

        insert_task(mongo_db, "test_state_cancelled", {
            "operator_path": str(script),
            "payload": {},
            "connection": {},
            "timeout_seconds": 20
        }, status="PENDING")
        mongo_db["schedules"].insert_one({"task_name": "test_state_cancelled", "state": "PENDING"})
        
        def run_it():
            try:
                execute_operator_task.apply(args=("test_state_cancelled",))
            except Exception:
                pass
                
        t = threading.Thread(target=run_it)
        t.start()
        time.sleep(1) # wait for process to start
        redis_client.set("cancel:test_state_cancelled", "1")
        t.join(timeout=5)
        
        task = mongo_db.queue_table.find_one({"task_name": "test_state_cancelled"})
        assert task["state"] == TaskState.CANCELLED.value

    def test_state_timed_out(self, mongo_db, tmp_path):
        from tasks import execute_operator_task
        
        script = tmp_path / "timeout_op.py"
        script.write_text("import time\nimport sys\nimport os\nsys.path.append(os.path.abspath('worker'))\nfrom base_operator import BaseOperator, run_operator\nclass DummyOp(BaseOperator):\n    def initialize(self, p, c): pass\n    def run(self): time.sleep(5)\n    def finish(self): pass\nif __name__ == '__main__':\n    run_operator(DummyOp(), {}, {})\n")

        insert_task(mongo_db, "test_state_timeout", {
            "operator_path": str(script),
            "payload": {},
            "connection": {},
            "timeout_seconds": 1
        }, status="PENDING")
        mongo_db["schedules"].insert_one({"task_name": "test_state_timeout", "state": "PENDING"})
        
        try:
            execute_operator_task.apply(args=("test_state_timeout",))
        except Exception:
            pass
            
        task = mongo_db.queue_table.find_one({"task_name": "test_state_timeout"})
        assert task["state"] == TaskState.TIMED_OUT.value

    def test_state_retry_and_exhausted(self, mongo_db, tmp_path):
        from tasks import execute_operator_task
        
        script = tmp_path / "fail_op.py"
        script.write_text("import sys\nimport os\nsys.path.append(os.path.abspath('worker'))\nfrom base_operator import BaseOperator, run_operator\nclass DummyOp(BaseOperator):\n    def initialize(self, p, c): pass\n    def run(self): raise ValueError('err')\n    def finish(self): pass\nif __name__ == '__main__':\n    run_operator(DummyOp(), {}, {})\n")

        insert_task(mongo_db, "test_state_retry", {
            "operator_path": str(script),
            "payload": {},
            "connection": {},
            "timeout_seconds": 10
        }, status="PENDING")
        mongo_db["schedules"].insert_one({"task_name": "test_state_retry", "state": "PENDING", "max_retries": 1, "num_of_retries": 0})
        
        try:
            # First attempt should yield RETRY
            execute_operator_task.apply(args=("test_state_retry",))
        except Exception as e:
            pass
            
        task = mongo_db.queue_table.find_one({"task_name": "test_state_retry"})
        assert task["state"] == TaskState.RETRY.value
        assert task["num_of_retries"] == 1
        
        try:
            # Second attempt should yield EXHAUSTED since max_retries is 1
            execute_operator_task.apply(args=("test_state_retry",), kwargs={}, task_id="some-id", retries=1)
        except Exception:
            pass
            
        task = mongo_db.queue_table.find_one({"task_name": "test_state_retry"})
        assert task["state"] == TaskState.EXHAUSTED.value


