import os
import json
import boto3
from celery import Celery
from celery.schedules import crontab
import redis
from botocore.exceptions import ClientError
from pymongo import MongoClient

app = Celery('tasks',
             broker='redis://localhost:6340/1',
             backend='redis://localhost:6340/2')

redis_list_client = redis.Redis(host='localhost', port=6340, db=0)

LAMBDA_REGION = os.getenv("AWS_REGION", "eu-north-1") 
lambda_client = boto3.client('lambda', region_name=LAMBDA_REGION)

@app.task
def check_redis_queue():
    """
    This is the periodic task that runs every 5 seconds.
    It checks the 'task_queue' list in Redis (db=0).
    """

    print("[Celery Beat]: Checking Redis 'batch_run'...")

    task_done=0
    while True:

        task_name_bytes = redis_list_client.rpop("batch_run")

        if not task_name_bytes:
            break
        
        task_name = task_name_bytes.decode('utf-8')
        print(f"[Celery Beat]: Found task! '{task_name}'. Sending to worker...")

        #print_the_name.delay(task_name)
        invoke_lambda_task.delay(task_name)

        #MARK IT AS COMPLETED
        mark_completed.delay(task_name)

        task_done+=1

    if task_done>0:
        print(f"[Celery beat] : no. of invoked task = {task_done} ")
    else:
        print("[Celery Beat]: Batch_run is empty...No tasks found.")

@app.task
def print_the_name(name):
    """
    This is the task that does the final work.
    It's called by 'check_redis_queue'.
    """
    print("-----------------------------------")
    print(f"[Celery Worker]: EXECUTING TASK: {name}")
    print("-----------------------------------")
    return f"Task {name} processed."



@app.task
def mark_completed(task_name):
    """
    This task updates the MongoDB 'queue_table' to mark a task as 'completed'.
    """
    print(f"[Celery Worker]: Marking task '{task_name}' as completed...")
    
    client = None
    try:
        client = MongoClient("mongodb://localhost:27017")
        db = client.tasks_db
        
        result = db.queue_table.update_one(
            {"task_name": task_name, "status": "running"}, 
            {"$set": {"status": "completed"}}
        )
        
        if result.matched_count > 0:
            print(f"[Celery Worker]: Successfully marked '{task_name}' as completed.")
        else:
            print(f"[Celery Worker]: WARNING: Could not find task '{task_name}' in 'running' state to mark as completed.")
            
    except Exception as e:
        print(f"[Celery Worker]: ERROR connecting to MongoDB: {e}")
    finally:
        if client:
            client.close()




@app.task
def invoke_lambda_task(task_name):
    """
    This is the task that does the final work.
    It's called by 'check_redis_queue'.
    It invokes (runs) an AWS Lambda function with the task name.
    """
    print("-----------------------------------")
    print(f"[Celery Worker]: EXECUTING TASK: Invoking Lambda for '{task_name}'")

    FUNCTION_NAME = "myCeleryTaskHandler" 

    payload = {
        "task_name": task_name  
    }

    try:
        response = lambda_client.invoke(
            FunctionName="taskScheduler",
            InvocationType='RequestResponse', 
            Payload=json.dumps(payload)      
        )

        response_payload = json.loads(response['Payload'].read().decode('utf-8'))

        print(f"[Celery Worker]: Lambda Response: {response_payload}")
        print("-----------------------------------")

        return f"Lambda invoked. Response: {response_payload}"

    except ClientError as e:
        print(f"[Celery Worker]: ERROR invoking Lambda: {e}")
        print("-----------------------------------")
        return f"Error: {e}"
    except Exception as e:
        print(f"[Celery Worker]: UNEXPECTED ERROR: {e}")
        print("-----------------------------------")
        return f"Error: {e}"


app.conf.beat_schedule = {
    'check-redis-every-5-seconds': {
        'task': 'tasks.check_redis_queue', 
        'schedule': 5.0,  
    },  
}
app.conf.timezone = 'Asia/Kolkata'