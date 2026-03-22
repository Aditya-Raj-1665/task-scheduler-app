import sys
import os
import pytest
from datetime import datetime, timezone, timedelta
from pymongo import MongoClient
import redis

# Ensure we can import from backend root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from fastapi.testclient import TestClient
from main import app

@pytest.fixture(scope="session")
def mongo_db():
    """Connect to MongoDB for cleanup during tests."""
    client = MongoClient("mongodb://localhost:27017")
    db = client.tasks_db
    yield db
    # Cleanup: delete anything inserted by test cases starting with 'test_'
    db.schedules.delete_many({"task_name": {"$regex": "^test_"}})
    db.queue_table.delete_many({"task_name": {"$regex": "^test_"}})
    client.close()

@pytest.fixture(scope="session")
def redis_client():
    """Connect to Redis for cleanup."""
    client = redis.Redis(host="localhost", port=6340, db=0)
    yield client
    # Cleanup: delete any cancel signals starting with test_
    for key in client.keys("cancel:test_*"):
        client.delete(key)

@pytest.fixture(scope="module")
def test_client():
    """
    By using 'with TestClient(app)', we tell FastAPI to trigger its @asynccontextmanager 'lifespan', 
    which connects FastAPI to MongoDB and Redis just like the real server!
    """
    with TestClient(app) as client:
        yield client

class TestAPI:

    def test_create_valid_task(self, test_client, mongo_db):
        now = datetime.now(timezone.utc)
        payload = {
            "task_name": "test_api_task_1",
            "cron": "*/5 * * * *",
            "start_date": now.isoformat(),
            "end_date": (now + timedelta(days=1)).isoformat(),
            "priority": 2,
            "max_retries": 1
        }
        
        # Act
        response = test_client.post("/tasks", json=payload)
        
        # Assert
        assert response.status_code == 200
        data = response.json()
        assert data["message"] == "Task schedule created"
        assert "first_run" in data
        
        # Verify it successfully inserted into the MongoDB 'schedules' collection
        doc = mongo_db.schedules.find_one({"task_name": "test_api_task_1"})
        assert doc is not None

    def test_create_invalid_cron_fails(self, test_client):
        now = datetime.now(timezone.utc)
        payload = {
            "task_name": "test_api_invalid_cron",
            "cron": "invalid_cron_string_here",
            "start_date": now.isoformat(),
            "end_date": (now + timedelta(days=1)).isoformat()
        }
        
        response = test_client.post("/tasks", json=payload)
        
        # The backend correctly throws HTTP 422 Unprocessable Entity due to Pydantic cron validation
        assert response.status_code == 422

    def test_get_all_tasks(self, test_client, mongo_db):
        now = datetime.now(timezone.utc)
        # 1. Seed a test task straight into the DB
        mongo_db.schedules.insert_one({
            "task_name": "test_api_fetch_1", 
            "priority": 1,
            "cron": "*/5 * * * *",
            "start_date": now,
            "end_date": now + timedelta(days=1)
        })
        
        # 2. Call the GET endpoint
        response = test_client.get("/tasks")
        assert response.status_code == 200
        tasks = response.json()
        
        # 3. Assert the task exists in the returned list
        assert isinstance(tasks, list)
        task_names = [t.get("task_name") for t in tasks]
        assert "test_api_fetch_1" in task_names

    def test_cancel_task(self, test_client, redis_client):
        # Fire cancel endpoint
        response = test_client.post("/tasks/test_custom_cancel_command/cancel")
        
        assert response.status_code == 200
        assert response.json()["message"] == "cancelled test_custom_cancel_command"
        
        # Verify redis string flag was actually set for the worker Watchdog to catch
        assert redis_client.exists("cancel:test_custom_cancel_command")

    # --- Edge Cases for POST /tasks ---

    def test_create_end_date_before_start_date(self, test_client):
        now = datetime.now(timezone.utc)
        payload = {
            "task_name": "test_api_invalid_dates",
            "cron": "*/5 * * * *",
            "start_date": now.isoformat(),
            "end_date": (now - timedelta(days=1)).isoformat()
        }
        
        response = test_client.post("/tasks", json=payload)
        
        # Pydantic validation kicks in and throws 422
        assert response.status_code == 422
        assert any("end_date must be after start_date" in err["msg"] for err in response.json()["detail"])

    def test_create_first_run_after_end_date(self, test_client):
        now = datetime.now(timezone.utc)
        payload = {
            "task_name": "test_api_late_first_run",
            "cron": "0 0 1 1 *", # Runs once a year on Jan 1st
            "start_date": now.isoformat(),
            # Make end_date only 1 day from now, guaranteed to miss the next Jan 1st
            "end_date": (now + timedelta(days=1)).isoformat()
        }
        
        response = test_client.post("/tasks", json=payload)
        
        # Custom logic in manager.py throws 400
        assert response.status_code == 400
        assert "First run time is after the end date" in response.json()["detail"]

    # --- Tests for GET /tasks/queue ---

    def test_get_empty_queue(self, test_client, mongo_db):
        # Clear out queue_table for baseline
        mongo_db.queue_table.delete_many({})
        
        response = test_client.get("/tasks/queue")
        assert response.status_code == 200
        assert response.json() == []

    def test_get_tasks_in_queue(self, test_client, mongo_db):
        now = datetime.now(timezone.utc)
        mongo_db.queue_table.insert_one({
            "task_name": "test_queue_task_1",
            "priority": 1,
            "next_run": now
        })
        
        response = test_client.get("/tasks/queue")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["task_name"] == "test_queue_task_1"
        assert "_id" in data[0]

    # --- Tests for DELETE /tasks/{task_id} ---

    def test_delete_task_success(self, test_client, mongo_db):
        # Insert dummy task
        result = mongo_db.schedules.insert_one({"task_name": "test_api_delete_me"})
        task_id = str(result.inserted_id)
        
        # Test deletion
        response = test_client.delete(f"/tasks/{task_id}")
        assert response.status_code == 200
        assert response.json() == {"message": "Task deleted successfully"}
        
        # Verify it's gone
        doc = mongo_db.schedules.find_one({"_id": result.inserted_id})
        assert doc is None

    def test_delete_non_existent_task(self, test_client):
        # Valid ObjectId format, but doesn't exist
        dummy_id = "507f1f77bcf86cd799439011"
        response = test_client.delete(f"/tasks/{dummy_id}")
        
        assert response.status_code == 404
        assert response.json()["detail"] == "Task not found"

    def test_delete_invalid_object_id(self, test_client):
        # Completely invalid hex format
        invalid_id = "not-a-valid-hex-24-char-string"
        response = test_client.delete(f"/tasks/{invalid_id}")
        
        assert response.status_code == 400
        assert response.json()["detail"] == "Invalid task ID format"
