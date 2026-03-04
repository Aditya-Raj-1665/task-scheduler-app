import os
import json
import boto3
from celery import Celery
from celery.schedules import crontab
import redis
from botocore.exceptions import ClientError
from pymongo import MongoClient
from bson.objectid import ObjectId
import subprocess
import sys
import threading
import time

mongo_client = MongoClient("mongodb://localhost:27017")
db = mongo_client.tasks_db

app = Celery('tasks',
             broker='redis://localhost:6340/1',
             backend='redis://localhost:6340/2')

redis_list_client = redis.Redis(host='localhost', port=6340, db=0)

LAMBDA_REGION = os.getenv("AWS_REGION", "eu-north-1") 
lambda_client = boto3.client('lambda', region_name=LAMBDA_REGION)

@app.task
def check_redis_queue():

    print("[Celery Beat]: Checking Redis 'batch_run'...")

    task_done=0
    while True:

        task_name_bytes = redis_list_client.rpop("batch_run")

        if not task_name_bytes:
            break
        
        task_name = task_name_bytes.decode('utf-8')
        print(f"[Celery Beat]: Found task! '{task_name}'. Sending to worker...")

        execute_operator_task.delay(task_name)
        # print_the_name.delay(task_name)
        #invoke_lambda_task.delay(task_name)

        #MARK IT AS COMPLETED
        # mark_completed.delay(task_name)

        task_done+=1

    if task_done>0:
        print(f"[Celery beat] : no. of invoked task = {task_done} ")
    else:
        print("[Celery Beat]: Batch_run is empty...No tasks found.")

@app.task
def print_the_name(task_name):
    print("-----------------------------------")
    print(f"[Celery Worker]: EXECUTING TASK: {task_name}")
    print("-----------------------------------")
    delete_task_from_queue_table_and_schedules_table.delay(task_name)
    return f"Task {task_name} processed."



@app.task
def delete_task_from_queue_table_and_schedules_table(task_name):
    db.queue_table.delete_one({"task_name" : task_name})
    db["schedules"].delete_one({"task_name" : task_name})
    print(f"delete kardiya {task_name}")


@app.task(bind=True, max_retries=3)
def execute_operator_task(self, task_name: str):
    """
    Reads task_config from MongoDB , spawns operator script as subprocess.
    Watchdog thread watches for cancel (Redis) or timeout
    """
    
    task_doc = db.queue_table.find_one({"task_name" : task_name})
    if not task_doc:
        print(f"[Worker] '{task_name}' not found in queue_table. Skipping.")
        return 
    
    config = task_doc.get("task_config", {})
    operator_path = config.get("operator_path")
    payload = config.get("payload", {})
    connection = config.get("connection", {})
    timeout_seconds = config.get("timeout_seconds", 3600)
    
    if not operator_path:
        print(f"[Worker] No operator_path in task_config for '{task_name}'. Skipping.")
        return
    
    import json
    process = subprocess.Popen(
        [sys.executable, operator_path,
         json.dumps(payload),
         json.dumps(connection)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text= True
    )
    
    print(f"[Worker] '{task_name}' started as PID : {process.pid}")


    cancelled = threading.Event()
    timed_out = threading.Event()
    run_done = threading.Event()
    
    def watchdog():
        deadline = time.monotomic() + timeout_seconds
        while not run_done.is_set():
            if redis_list_client.exists(f"cancel:{task_name}"):
                print(f"[WatchDog]  cancel for '{task_name}'. Terminating.")
                redis_list_client.delete(f"cancel:{task_name}")
                cancelled.set()
                process.terminate()
                return
            
            if time.monotonic() > deadline:
                print(f"[Watchdog] Timeout for '{task_name}'. Terminating.")    
                timed_out.set()
                process.terminate()
                return
            
            time.sleep(1)
            
    watchdog_thread = threading.Thread(target= watchdog, daemon= True)
    watchdog_thread.start()
    
    stdout, stderr = process.communicate()
    run_done.set()
    watchdog_thread.join(timeout=5)
    
    exit_code= process.returncode
    print(f"[Worker] '{task_name}' exited with code {exit_code}")

    if stdout: print(f"[Worker] STDOUT:\n{stdout}")
    if stderr: print(f"[Worker] STDERR:\n{stderr}")
    
    if exit_code==0 :
        delete_task_from_queue_table_and_schedules_table.delay(task_name)
    elif cancelled.is_set() or timed_out.is_set():
        db.queue_table.update_one(
            {"task_name" : task_name},
            {"$set" : {"status" : "FAILED"}}
        )
    else:
        raise self.retry(countdown =30)

@app.task
def mark_completed(task_name):
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
    print("-----------------------------------")
    print(f"[Celery Worker]: EXECUTING TASK: Invoking Lambda for '{task_name}'")

    FUNCTION_NAME = "taskScheduler" 

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



'''
To test mannually:

'''