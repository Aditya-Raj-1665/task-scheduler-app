from pydantic import BaseModel, Field
from datetime import datetime

class TaskSchedule(BaseModel):
    name: str
    cron: str  # "*/1 * * * *" 
    start_date: datetime
    end_date: datetime
    priority: int = 3

    class Config:
        arbitrary_types_allowed = True
        json_encoders = {datetime: lambda dt: dt.isoformat()}

class TaskInDB(TaskSchedule):
    id: str = Field(..., alias="_id")
    next_run: datetime