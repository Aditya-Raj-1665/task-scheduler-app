from celery import Celery
from celery.schedules import crontab
import redis

app = Celery('tasks',
             broker='redis://localhost:6340/1',
             backend='redis://localhost:6340/2')

redis_list_client = redis.Redis(host='localhost', port=6340, db=0)

@app.task
def check_redis_queue():
    """
    This is the periodic task that runs every 5 seconds.
    It checks the 'task_queue' list in Redis (db=0).
    """
    print("[Celery Beat]: Checking Redis 'task_queue'...")

    task_name_bytes = redis_list_client.rpop("task_queue")

    if task_name_bytes:
        task_name = task_name_bytes.decode('utf-8')
        print(f"[Celery Beat]: Found task! '{task_name}'. Sending to worker...")

        print_the_name.delay(task_name)
    else:
        print("[Celery Beat]: No tasks found.")

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

app.conf.beat_schedule = {
    'check-redis-every-5-seconds': {
        'task': 'tasks.check_redis_queue', 
        'schedule': 5.0,  
    },
}
app.conf.timezone = 'UTC'