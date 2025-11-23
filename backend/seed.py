import os
from pymongo import MongoClient
from datetime import datetime, timedelta, timezone
from croniter import croniter

# --- CONFIGURATION ---
MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "tasks_db"
COLLECTION_NAME = "schedules"
NUM_TASKS_TO_CREATE = 100
# ---------------------

def seed_database():
    print("🌱 Starting database seeding process...")
    
    try:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]
    except Exception as e:
        print(f"❌ ERROR: Could not connect to MongoDB. Is it running? \n{e}")
        return

    now = datetime.now(timezone.utc)
    start_date = now - timedelta(minutes=1) 
    end_date = now + timedelta(days=1)
    cron_string = "*/1 * * * *"
    
    tasks_to_insert = []
    
    for i in range(NUM_TASKS_TO_CREATE):
        task_name = f"Load Test Task {i + 1}"

        task_priority = (i % 5) + 1

        
        try:
            first_run = croniter(cron_string, now).get_next(datetime)
        except Exception as e:
            print(f"Error calculating cron: {e}")
            continue
            
        task_doc = {
            "task_name": task_name,
            "cron": cron_string,
            "start_date": start_date,
            "end_date": end_date,
            "next_run": first_run,
            "priority": task_priority
            # --- THIS INCONSISTENT LINE IS NOW REMOVED ---
            # "schedule_status": "active"
        }
        tasks_to_insert.append(task_doc)

    print(f"📝 Created {len(tasks_to_insert)} task documents. Inserting into MongoDB...")

    try:
        collection.delete_many({"name": {"$regex": "Load Test Task"}})
        result = collection.insert_many(tasks_to_insert)
        
        print(f"✅ Success! Inserted {len(result.inserted_ids)} tasks.")
    except Exception as e:
        print(f"❌ ERROR: Could not insert tasks into MongoDB. \n{e}")
    finally:
        client.close()
        print("🌱 Seeding complete. Connection closed.")

if __name__ == "__main__":
    seed_database()