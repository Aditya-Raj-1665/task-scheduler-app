import asyncio
from datetime import datetime, timezone
from croniter import croniter
from bson import ObjectId
from fastapi import HTTPException
import json
import os

from models import TaskInput, TaskInDB


class TaskManager:
    """Core scheduler engine that manages task lifecycle.

    Handles task creation, scheduling, queue management, and database operations.
    Reads configuration from config.json for parallelism settings.
    """

    def __init__(self, db, redis_client):
        """Initialize TaskManager with database and cache connections.

        Args:
            db: Motor async MongoDB database instance.
            redis_client: Async Redis client instance.
        """
        self.db = db
        self.redis = redis_client

        config_path = os.path.join(os.path.dirname(__file__), 'config.json')
        with open(config_path, 'r') as f:
            config = json.load(f)
            self.max_parallelism = config.get("max_parallelism", 10)

    async def date_time_criteria(self):
        """Query MongoDB for tasks that are due to run right now.

        Finds tasks where the current UTC time falls within [start_date, end_date]
        and next_run is at or before the current time.

        Returns:
            List of dicts with task_name and priority for each due task.
        """
        current_time = datetime.now(timezone.utc)

        date_and_time_criteria = {
            "start_date": {"$lte": current_time},
            "end_date": {"$gte": current_time},
            "next_run": {"$lte": current_time},
            "state": "PENDING"
        }

        attributes_to_include_in_list = {
            "task_name": 1,
            "priority": 1
        }

        tasks_ready = await self.db.schedules.find(
            date_and_time_criteria, attributes_to_include_in_list
        ).to_list(length=None)
        return tasks_ready

    async def run_scheduler_loop(self):
        """Background loop that polls for due tasks every 60 seconds.

        Queries for tasks matching date/time criteria, passes them through the
        queue manager for priority sorting, and pushes eligible task names into
        the Redis 'batch_run' list (respecting max_parallelism).
        """
        while True:
            print("starting the loop...")
            tasks_ready = await self.date_time_criteria()
            prioritized_tasks = await self.fun_queue_manager(tasks_ready)

            task_names = [t["task_name"] for t in prioritized_tasks]

            if task_names:
                for t in task_names:
                    current_redis_length = await self.redis.llen("batch_run")
                    if current_redis_length < self.max_parallelism:
                        await self.redis.lpush("batch_run", t)

            else:
                print("no task to put in redis")

            await asyncio.sleep(60)

    async def fun_queue_manager(self, tasks_ready):
        """Insert new tasks into queue_table and return top priority tasks.

        Compares incoming ready tasks against what's already in the queue_table,
        inserts only new ones, then returns the top N tasks sorted by priority
        (where N = max_parallelism).

        Args:
            tasks_ready: List of task dicts from date_time_criteria().

        Returns:
            List of task dicts sorted by priority, limited to max_parallelism.
        """
        all_queued = await self.db.queue_table.find().to_list(length=None)
        new_tasks = [item for item in tasks_ready if item not in all_queued]

        if new_tasks:
            await self.db.queue_table.insert_many(new_tasks)

        prioritized = await self.db.queue_table.find().sort(
            "priority", 1
        ).limit(self.max_parallelism).to_list(length=None)

        return prioritized

    async def create_schedule(self, task: TaskInput):
        print(f"[TaskManager]: Creating new schedule: {task.task_name}")
        
        """Create a new task schedule in MongoDB.

        Validates the cron expression, calculates the first run time, and
        inserts the task document into the schedules collection.

        Args:
            task: Validated TaskInput from the API request.

        Returns:
            Dict with success message and computed first_run datetime.

        Raises:
            HTTPException: If the cron string is invalid or first run is after end_date.
        """
        now = datetime.now(timezone.utc)
        start_date_utc = task.start_date.astimezone(timezone.utc)
        iterator_start_time = max(now, start_date_utc)

        try:
            first_run = croniter(task.cron, iterator_start_time).get_next(datetime)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Invalid cron string: {e}")

        if first_run > task.end_date.astimezone(timezone.utc):
            raise HTTPException(
                status_code=400,
                detail="First run time is after the end date. Task will never run."
            )

        task_doc = task.model_dump()
        task_doc["next_run"] = first_run

        await self.db.schedules.insert_one(task_doc)
        return {"message": "Task schedule created", "first_run": first_run}

    async def get_all_schedules(self):
        """Fetch all task schedules from the MongoDB schedules collection.

        Returns:
            List of TaskInDB instances for every document in the collection.
        """
        tasks_list = []
        due_tasks_cursor = self.db.schedules.find({})
        async for task in due_tasks_cursor:
            tasks_list.append(TaskInDB.from_mongo(task))
        return tasks_list

    async def get_queued_tasks(self):
        """Fetch all tasks currently in the queue_table.

        Returns:
            List of task dicts with _id converted to string for JSON serialization.
        """
        tasks = await self.db.queue_table.find().to_list(length=None)
        for task in tasks:
            task["_id"] = str(task["_id"])
        return tasks

    async def delete_schedule(self, task_id: str):
        """Delete a task schedule by its MongoDB ObjectId.

        Args:
            task_id: String representation of the MongoDB ObjectId.

        Returns:
            Dict with success message.
            
        Raises:
            HTTPException: If the ID format is invalid or the task is not found.
        """
        from bson import ObjectId
        from fastapi import HTTPException
        try:
            object_id_to_delete = ObjectId(task_id)
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid task ID format")

        delete_result = await self.db.schedules.delete_one({"_id": object_id_to_delete})

        if delete_result.deleted_count == 1:
            return {"message": "Task deleted successfully"}
        else:
            raise HTTPException(status_code=404, detail="Task not found")
        
    async def pause_task(self, task_name: str):
        """Pause a scheduled task by updating state to PAUSED."""
        from fastapi import HTTPException
        result = await self.db.schedules.update_one({"task_name": task_name}, {"$set": {"state": "PAUSED"}})
        await self.db.queue_table.update_one({"task_name": task_name}, {"$set": {"state": "PAUSED"}})
        if result.matched_count == 0:
            raise HTTPException(status_code=404, detail="Task not found.")
        return {"message": f"Task {task_name} paused successfully"}

    async def resume_task(self, task_name: str):
        """Resume a paused task by updating state to PENDING."""
        from fastapi import HTTPException
        result = await self.db.schedules.update_one({"task_name": task_name}, {"$set": {"state": "PENDING"}})
        await self.db.queue_table.update_one({"task_name": task_name}, {"$set": {"state": "PENDING"}})
        if result.matched_count == 0:
            raise HTTPException(status_code=404, detail="Task not found.")
        return {"message": f"Task {task_name} resumed successfully"}

    async def run_task_adhoc(self, task_name: str):
        """Trigger a task immediately by pushing it to Redis directly."""
        from fastapi import HTTPException
        task = await self.db.schedules.find_one({"task_name": task_name})
        if not task:
            raise HTTPException(status_code=404, detail="Task not found in schedules")
        
        # Upsert it into queue table so execution logic finds it there
        task.pop('_id', None) # remove _id if exists
        task['state'] = 'PENDING'
        await self.db.queue_table.update_one(
            {"task_name": task_name},
            {"$set": task},
            upsert=True
        )
        
        await self.redis.lpush("batch_run", task_name)
        return {"message": f"Task {task_name} queued for ad-hoc execution"}

    async def fun_done(self):
        """Background loop for cleanup if needed in future.
        Currently doing nothing to preserve completed tasks.
        """
        while True:
            await asyncio.sleep(86400)



