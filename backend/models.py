from pydantic import BaseModel, Field, field_validator
from datetime import datetime 
from typing import Dict, Any,Optional

class TaskSchedule(BaseModel):
    '''
    Base model with common fields
    '''
    task_name: str = Field(
        ...,
        min_length=1,
        max_length=200,
        description="Name of the Task"
    )
    
    cron: str = Field(
        ...,
        description="cron expression for scheduling"
    ) 
    
    description: Optional[str] = Field(
        default=None,
        max_length=1000,
        description="description of the task. OR what this Task does?"
    )
    
    start_date: datetime = Field(
        ...,
        description="when the task schedule should start"
    )
    
    end_date: datetime = Field(
        ...,
        description="when the task scheduling should end"
    )
    
    priority: int = Field(
        default=3,
        ge=1,
        le=3,
        description="priority of the task (lower the numurical value of priority => more important or higher priority )"
    )
    
    max_retries: int = Field(
        default=3,
        ge=0,
        le=5,
        description="number of retries before the task stops scheduling"
    )
    
    task_config: Dict[str, Any] = Field(
        default_factory = dict,
        description="specific task configurations"
    )
    
    state : str =  Field(
        ...,
        description = "current state of the task - READY , PENDING , RUNNING , COMPLETED"
    )

    class Config:
        arbitrary_types_allowed = True
        json_encoders = {datetime: lambda dt: dt.isoformat()}

class TaskInDB(TaskSchedule):
    id: str = Field(..., alias="_id")
    next_run: datetime