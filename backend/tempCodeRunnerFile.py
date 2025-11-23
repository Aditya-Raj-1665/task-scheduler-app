import asyncio
from fastapi import FastAPI, HTTPException
from motor.motor_asyncio import AsyncIOMotorClient
from redis.asyncio import Redis
from contextlib import asynccontextmanager
from fastapi.middleware.cors import CORSMiddleware 
from typing import List
from manager import TaskManager
from models import TaskSchedule,TaskInDB


mongo_client = AsyncIOMotorClient("mongodb://localhost:27017")
db = mongo_client.tasks_db 
redis_client = Redis(host="localhost", port=6340, db=0, decode_responses=True)

app = TaskManager(db, redis_client)
async def main_runner():
    # list_test2=await app.date_time_criteria()
    list_test=await app.run_scheduler_loop()
    # list_test=await app.fun_queue_manager(list_test2)

    for task in list_test:
        print(task)
if __name__ == "__main__":
    asyncio.run(main_runner())