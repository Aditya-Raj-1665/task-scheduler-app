import asyncio
from fastapi import FastAPI, HTTPException
from motor.motor_asyncio import AsyncIOMotorClient
from redis.asyncio import Redis
from contextlib import asynccontextmanager
from fastapi.middleware.cors import CORSMiddleware
from typing import List
from manager import TaskManager
from models import TaskInput, TaskInDB

db = None
redis_client = None
task_manager = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application startup and shutdown lifecycle.

    Connects to MongoDB and Redis on startup, initializes the TaskManager,
    and starts the background scheduler loop. Closes all connections on shutdown.
    """
    global db, redis_client, task_manager
    print("Connecting to databases...")

    mongo_client = AsyncIOMotorClient("mongodb://localhost:27017")
    db = mongo_client.tasks_db

    redis_client = Redis(host="localhost", port=6340, db=0, decode_responses=True)
    print("Connections successful.")

    task_manager = TaskManager(db, redis_client)

    print("Starting background loops...")
    asyncio.create_task(task_manager.run_scheduler_loop())
    yield

    print("Closing connections...")
    mongo_client.close()
    await redis_client.close()
    print("Shutdown complete.")

app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.post("/tasks/{task_name}/cancel")
async def cancel_task(task_name: str):
    """Cancel a running task by setting a Redis flag for the worker watchdog.

    Args:
        task_name: Name of the task to cancel.

    Returns:
        Confirmation message.
    """
    await redis_client.set(f"cancel:{task_name}", "1")
    return {"message": f"cancelled {task_name}"}


@app.post("/tasks")
async def create_task(task: TaskInput):
    """Create a new scheduled task.

    Args:
        task: Task input data including name, cron expression, and date range.

    Returns:
        Confirmation message with the computed first run time.
    """
    return await task_manager.create_schedule(task)


@app.get("/tasks", response_model=List[TaskInDB])
async def get_all_tasks():
    """Retrieve all task schedules from the database.

    Returns:
        List of all task documents.
    """
    return await task_manager.get_all_schedules()


@app.get("/tasks/queue")
async def get_tasks_in_queue():
    """Retrieve all tasks currently in the execution queue.

    Returns:
        List of queued task documents.
    """
    return await task_manager.get_queued_tasks()


@app.post("/tasks/{task_name}/pause")
async def pause_task(task_name: str):
    """Pause a schedule."""
    return await task_manager.pause_task(task_name)

@app.post("/tasks/{task_name}/resume")
async def resume_task(task_name: str):
    """Resume a schedule."""
    return await task_manager.resume_task(task_name)

@app.post("/tasks/{task_name}/run")
async def run_task_adhoc(task_name: str):
    """Trigger ad-hoc execution of a task."""
    return await task_manager.run_task_adhoc(task_name)

@app.delete("/tasks/{task_id}")
async def delete_task(task_id: str):
    """Delete a task schedule by its MongoDB ObjectId.

    Args:
        task_id: The string representation of the MongoDB ObjectId.

    Returns:
        Confirmation message on success.
    """
    return await task_manager.delete_schedule(task_id)
