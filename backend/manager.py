import asyncio
from datetime import datetime, timezone
from croniter import croniter
from bson import ObjectId
from fastapi import HTTPException
import json
import os

from models import TaskSchedule, TaskInDB

class TaskManager:
    def __init__(self, db, redis_client):
        self.db = db
        self.redis = redis_client

        config_path = os.path.join(os.path.dirname(__file__), 'config.json')
        try:
            with open(config_path, 'r') as f:
                config = json.load(f)
                self.BATCH_SIZE = config.get("BATCH_SIZE", 10)
            print(f"TaskManager config loaded. BATCH_SIZE = {self.BATCH_SIZE}")

        print("TaskManager initialized.")


    async def create_schedule(self, task: TaskSchedule):
        print(f"[TaskManager]: Creating new schedule: {task.name}")
        now = datetime.now(timezone.utc)
        start_date_utc = task.start_date.astimezone(timezone.utc)
        iterator_start_time = max(now, start_date_utc)

        try:
            first_run = croniter(task.cron, iterator_start_time).get_next(datetime)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Invalid cron string: {e}")

        if first_run > task.end_date.astimezone(timezone.utc):
            raise HTTPException(status_code=400, detail="First run time is after the end date. Task will never run.")

        task_doc = task.model_dump()
        task_doc["next_run"] = first_run

        await self.db.schedules.insert_one(task_doc)
        return {"message": "Task schedule created", "first_run": first_run}


    async def get_all_schedules(self):
        """
        Fetches all task schedules from the MongoDB database.
        """
        tasks_list = []
        due_tasks_cursor = self.db.schedules.find({})
        async for task in due_tasks_cursor:
            tasks_list.append(TaskInDB(**task_dict))
        return tasks_list


    async def delete_schedule(self, task_id: str):
        try:
            object_id_to_delete = ObjectId(task_id)
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid task ID format")

        delete_result = await self.db.schedules.delete_one({"_id": object_id_to_delete})
        
        if delete_result.deleted_count == 1:
            return {"message": "Task deleted successfully"}
        else:
            raise HTTPException(status_code=404, detail="Task not found")


    #FUNC_FIND
    async def fun_find(self):
        print("[TaskManager]: Fetching 'task_queue' from Redis...")
        tasks_in_queue = await self.redis.lrange("task_queue", 0, -1)
        return tasks_in_queue

    async def run_scheduler_loop(self):
        while True:
            print(f"[TaskManager]: Waking up to check for due tasks... {datetime.now(timezone.utc)}")
            now = datetime.now(timezone.utc)

            due_tasks_cursor = self.db.schedules.find({
                "next_run": {"$lte": now},
                "end_date": {"$gte": now}
            })

            async for task in due_tasks_cursor:
                end_date_utc = task["end_date"].astimezone(timezone.utc)

                if end_date_utc < now:
                    print(f"[TaskManager]: Task '{task['name']}' is past its end date. Skipping.")
                    continue

                task_name = task["name"]
                task_id = task["_id"]
                task_cron = task["cron"]
                task_priority = task.get("priority", 3)

                print(f"[TaskManager Loop 1]: Found due task: {task_name} (Priority: {task_priority})")

                task_data = json.dumps({
                    "name": task_name,
                    "priority": task_priority
                })

                await self.redis.lpush("task_queue", task_data)
                print(f"[TaskManager]: Pushed '{task_name}' to Redis 'task_queue'")

                try:
                    new_next_run = croniter(task_cron, now).get_next(datetime)

                    if new_next_run <= end_date_utc:
                        await self.db.schedules.update_one(
                            {"_id": task_id},
                            {"$set": {"next_run": new_next_run}}
                        )
                        print(f"[TaskManager]: Updated '{task_name}' next run to {new_next_run}")
                    else:
                        far_future = datetime(2099, 1, 1, tzinfo=timezone.utc)
                        await self.db.schedules.update_one(
                            {"_id": task_id},
                            {"$set": {"next_run": far_future}}
                        )
                        print(f"[TaskManager]: Task '{task_name}' has finished its schedule.")

                except Exception as e:
                    print(f"Error updating cron for {task_name}: {e}")
            #--60-- seconds delay
            await asyncio.sleep(60)


    async def fun_queue_manager(self):
        while True:
            print("[TaskManager Loop 2]: Waking up to manage batch queue...")
                        
            tasks_from_redis = await self.fun_find()
            
            if not tasks_from_redis:
                print("[TaskManager]: Redis 'task_queue' is empty. Nothing to move.")
            else:
                print(f"[TaskManager]: Found {len(tasks_from_redis)} tasks in Redis. Moving to MongoDB 'queue_table'...")
                for task_name in tasks_from_redis:
                    try:
                        task_info = json.loads(task_name)
                        task_name = task_info["name"]
                        task_priority = task_info.get("priority", 3)
                        
                        await self.db.queue_table.update_one(
                            {"task_name": task_name},
                            {"$set": {
                                "status": "pending",
                                "priority": task_priority, 
                                "last_updated": datetime.now(timezone.utc)
                            }},
                            upsert=True
                        )
                    except json.JSONDecodeError:
                        print(f"Error decoding task JSON: {task_name}")
                    
                await self.redis.delete("task_queue")
                print(f"[TaskManager]: Moved {len(tasks_from_redis)} tasks and cleared Redis 'task_queue'.")

            #---picking batch of 10---
            try:
                current_size = await self.redis.llen("batch_run")

                room_available = self.BATCH_SIZE - current_size

                print(f"[TaskManager Loop 2]: 'batch_run' has {current_size} tasks. Room for {room_available} more.")

                if room_available <= 0:
                    print("[TaskManager Loop 2]: 'batch_run' is full. No new tasks will be added.")


                if room_available > 0:
                    print(f"[TaskManager Loop 2]: Selecting up to {room_available} pending tasks from 'queue_table'...")

                    batch_cursor = self.db.queue_table.find(
                        {"status": "pending"}
                    ).sort("priority", 1).limit(room_available)
            
                    batch = await batch_cursor.to_list(room_available)

                    if batch:
                        print(f"[TaskManager Loop 2]: Selected {len(batch)} tasks. Pushing to 'batch_run'...")
                        task_names_in_batch = [t["task_name"] for t in batch]

                        await self.db.queue_table.update_many(
                            {"task_name": {"$in": task_names_in_batch}},
                            {"$set": {"status": "running", "last_updated": datetime.now(timezone.utc)}}
                        )
                        
                        #--pushing tasks in batch--
                        for task_name in task_names_in_batch:
                            await self.redis.lpush("batch_run", task_name)
                        
                        print(f"[TaskManager Loop 2]: Pushed {len(batch)} tasks to 'batch_run'.")
                    else:
                        print("[TaskManager Loop 2]: No pending tasks in 'queue_table' to add.")

            except Exception as e:
                print(f"[TaskManager Loop 2]: ERROR during batching: {e}")

            await asyncio.sleep(60)


    async def fun_done(self):
            while True:
                await asyncio.sleep(30) 
                
                print(f"[TaskManager Loop 3]: Waking up to clean 'queue_table' of completed tasks...")
                
                try:
                    delete_result = await self.db.queue_table.delete_many(
                        {"status": "completed"}
                    )
                    if delete_result.deleted_count > 0:
                        print(f"[TaskManager Loop 3]: Cleaned up {delete_result.deleted_count} completed tasks.")
                    else:
                        print(f"[TaskManager Loop 3]: No completed tasks to clean up.")
                except Exception as e:
                    print(f"[TaskManager Loop 3]: ERROR during cleanup: {e}")


