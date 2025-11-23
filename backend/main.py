import asyncio
from fastapi import FastAPI, HTTPException
from motor.motor_asyncio import AsyncIOMotorClient
from redis.asyncio import Redis
from contextlib import asynccontextmanager
from fastapi.middleware.cors import CORSMiddleware 
from typing import List
from manager import TaskManager
from models import TaskSchedule,TaskInDB


# mongo_client = AsyncIOMotorClient("mongodb://localhost:27017")
# db = mongo_client.tasks_db 
# redis_client = Redis(host="localhost", port=6340, db=0, decode_responses=True)

# app = TaskManager(db, redis_client)
# async def main_runner():
#     # list_test2=await app.date_time_criteria()
#     list_test=await app.run_scheduler_loop()
#     # list_test=await app.fun_queue_manager(list_test2)

#     for task in list_test:
#         print(task)
# if __name__ == "__main__":
#     asyncio.run(main_runner())



db = None
redis_client = None
task_manager = None 


@asynccontextmanager
async def lifespan(app: FastAPI):
    global db, redis_client, task_manager
    print("Connecting to databases...")
    
    mongo_client = AsyncIOMotorClient("mongodb://localhost:27017")
    db = mongo_client.tasks_db 

    redis_client = Redis(host="localhost", port=6340, db=0, decode_responses=True)
    print("Connections successful.")

    task_manager = TaskManager(db, redis_client)

    print("Starting background loops...")
    asyncio.create_task(task_manager.run_scheduler_loop())
    # asyncio.create_task(task_manager.fun_queue_manager())
    # asyncio.create_task(task_manager.fun_done())
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

# @app.post("/tasks")
# async def create_task(task: TaskSchedule):
#     return await task_manager.create_schedule(task)

# @app.get("/tasks", response_model=List[TaskInDB])
# async def get_all_tasks():
#     return await task_manager.get_all_schedules()

# @app.get("/tasks/queue", response_model=List[str])
# async def get_tasks_in_queue():
#     return await task_manager.fun_find()

# @app.delete("/tasks/{task_id}")
# async def delete_task(task_id: str):
#     return await task_manager.delete_schedule(task_id)
