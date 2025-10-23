import asyncio
from datetime import datetime, timezone
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from motor.motor_asyncio import AsyncIOMotorClient
from redis.asyncio import Redis
from croniter import croniter
from contextlib import asynccontextmanager
from bson import ObjectId 

from fastapi.middleware.cors import CORSMiddleware 

db = None
redis_client = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global db, redis_client
    print("Connecting to databases...")
    mongo_client = AsyncIOMotorClient("mongodb://localhost:27017")
    db = mongo_client.tasks_db 

    redis_client = Redis(host="localhost", port=6340, db=0, decode_responses=True)
    print("Connections successful.")

    print("Starting periodic task checker...")
    asyncio.create_task(periodic_task_checker())

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

class TaskSchedule(BaseModel):
    name: str
    cron: str  # "*/1 * * * *" 
    start_date: datetime
    end_date: datetime

    class Config:
        arbitrary_types_allowed = True
        json_encoders = {datetime: lambda dt: dt.isoformat()}

class TaskInDB(TaskSchedule):
    id: str = Field(..., alias="_id")
    next_run: datetime 


@app.post("/tasks")
async def create_task(task: TaskSchedule):
    """
    Receives a new task schedule from the React form.
    Calculates its first run time and saves it to MongoDB.
    """
    print(f"Received new task: {task.name}")

    now = datetime.now(timezone.utc)

    start_date_utc = task.start_date.astimezone(timezone.utc)

    iterator_start_time = max(now, start_date_utc)

    try:
        first_run = croniter(task.cron, iterator_start_time).get_next(datetime)
    except Exception as e:
        return {"error": f"Invalid cron string: {e}"}

    if first_run > task.end_date.astimezone(timezone.utc):
        return {"error": "First run time is after the end date. Task will never run."}

    task_doc = task.model_dump()
    task_doc["next_run"] = first_run

    await db.schedules.insert_one(task_doc)
    return {"message": "Task schedule created", "first_run": first_run}

@app.get("/tasks")
async def get_all_tasks():
    """
    Fetches all task schedules from the MongoDB database.
    """
    tasks_list = []
    due_tasks_cursor = db.schedules.find({})

    async for task in due_tasks_cursor:
        task["_id"] = str(task["_id"])
        tasks_list.append(task)
        
    return tasks_list

@app.delete("/tasks/{task_id}")
async def delete_task(task_id: str):
    """
    Deletes a specific task from MongoDB using its _id.
    """
    try:
        object_id_to_delete = ObjectId(task_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid task ID format")

    delete_result = await db.schedules.delete_one({"_id": object_id_to_delete})
    
    if delete_result.deleted_count == 1:
        return {"message": "Task deleted successfully"}
    else:
        raise HTTPException(status_code=404, detail="Task not found")

        

async def periodic_task_checker():
    """
    This function runs in the background for the entire
    life of the FastAPI application.
    """
    while True:
        print(f"[FastAPI Checker]: Waking up to check for due tasks... {datetime.now(timezone.utc)}")
        now = datetime.now(timezone.utc)

        due_tasks_cursor = db.schedules.find({
            "next_run": {"$lte": now},
            "end_date": {"$gte": now}
        })

        async for task in due_tasks_cursor:
            end_date_utc = task["end_date"].astimezone(timezone.utc)

            if end_date_utc < now:
                print(f"[FastAPI Checker]: Task '{task['name']}' is past its end date. Skipping.")
                continue

            task_name = task["name"]
            task_id = task["_id"]
            task_cron = task["cron"]

            print(f"[FastAPI Checker]: Found due task: {task_name}")

            await redis_client.lpush("task_queue", task_name)
            print(f"[FastAPI Checker]: Pushed '{task_name}' to Redis 'task_queue'")

            try:
                new_next_run = croniter(task_cron, now).get_next(datetime)

                if new_next_run <= end_date_utc:
                    await db.schedules.update_one(
                        {"_id": task_id},
                        {"$set": {"next_run": new_next_run}}
                    )
                    print(f"[FastAPI Checker]: Updated '{task_name}' next run to {new_next_run}")
                else:
                    far_future = datetime(2099, 1, 1, tzinfo=timezone.utc)
                    await db.schedules.update_one(
                        {"_id": task_id},
                        {"$set": {"next_run": far_future}}
                    )
                    print(f"[FastAPI Checker]: Task '{task_name}' has finished its schedule.")

            except Exception as e:
                print(f"Error updating cron for {task_name}: {e}")

        await asyncio.sleep(60)