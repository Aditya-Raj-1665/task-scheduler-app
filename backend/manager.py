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
        with open(config_path, 'r') as f:
            config = json.load(f)
            self.max_parallelism = config.get("max_parallelism", 10)
            # print(self.max_parallelism)
        
        #self.db
        #self.redis
        #self.max_parallelism



    #FUNC_FIND
    async def date_time_criteria(self):
        current_time = datetime.now()

        date_and_time_criteria ={
            "start_date":{
                "$lte": current_time
            },
            "end_date":{
                "$gte": current_time
            }
        }

        attributes_to_include_in_list = {
            "task_name":1,
            "priority":1
        }

        tasks_ready_to_run_based_on_date_condition = await self.db.schedules.find(date_and_time_criteria,attributes_to_include_in_list).to_list(length=None)
        return tasks_ready_to_run_based_on_date_condition




    async def run_scheduler_loop(self):
        MAX_PARALLELISM = self.max_parallelism
        
        while True:
            print("starting the loop...")
            
            tasks_ready_to_run_based_on_date_condition = await self.date_time_criteria()


            tasks_ready_to_run_based_on_date_condition_and_priority_and_max_parallelism = await self.fun_queue_manager(tasks_ready_to_run_based_on_date_condition)

            tasks_names_to_push_in_redis = [t["task_name"] for t in tasks_ready_to_run_based_on_date_condition_and_priority_and_max_parallelism]

            if tasks_names_to_push_in_redis:    
                for t in tasks_names_to_push_in_redis:

                    current_redis_length = await self.redis.llen("batch_run")

                    if(current_redis_length < MAX_PARALLELISM):
                        await self.redis.lpush("batch_run", t)

            else:
                print("no task to put in redis")

            # return tasks_names_to_push_in_redis

            # print("will run after 60 seconds")
            await asyncio.sleep(2)



    async def fun_queue_manager(self,tasks_ready_to_run_based_on_date_condition : list):

        all_tasks_in_queue_table = await self.db.queue_table.find().to_list(length=None) 

        # new_tasks_for_queue_table = tasks_ready_to_run_based_on_date_condition - all_tasks_in_queue_table
        new_tasks_for_queue_table = [item for item in tasks_ready_to_run_based_on_date_condition if item not in all_tasks_in_queue_table]
        
        if new_tasks_for_queue_table:
            await self.db.queue_table.insert_many(new_tasks_for_queue_table)


        MAX_PARALLELISM = self.max_parallelism
        tasks_ready_to_run_based_on_date_condition_and_priority_and_max_parallelism = await self.db.queue_table.find().sort("priority", 1).limit(MAX_PARALLELISM).to_list(length=None)

        return tasks_ready_to_run_based_on_date_condition_and_priority_and_max_parallelism



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


