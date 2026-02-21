from pydantic import BaseModel, Field, field_validator, model_validator
from datetime import datetime
from typing import Dict, Any, Optional
from enum import Enum

class TaskState(str,Enum):
    PENDING = "PENDING"             #sitting in DB1
    READY = "READY"                 #in DB2 
    RUNNING = "RUNNING"             #currently executing
    COMPLETED = "COMPLETED"         #success
    FAILED = "FAILED"               #for retry mechanism

# user input task model
class TaskInput(BaseModel):
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
    
    @model_validator(mode = "after")
    def end_date_must_be_after_start_date(self) -> "TaskInput":
        if self.end_date <=self.start_date:
            raise ValueError("end_must be after start_date")
        return self

    class Config:
        # arbitrary_types_allowed = True
        json_encoders = {datetime: lambda dt: dt.isoformat()}



class TaskInDB(TaskInput):
    id: str = Field(..., alias="_id")
    next_run: datetime
    
class TaskInDB(TaskInput):
    """
    Full task document stored in MongoDB.
    Extends TaskInput with all internal tracking fields.
    """
    id: str = Field(..., alias="_id")

    state: TaskState = Field(
        default=TaskState.PENDING
    )
    next_run: datetime = Field(
        ...,
        description="Next scheduled execution time (computed from cron)"
    )
    num_of_retries: int = Field(
        default=0,
        description="How many retries have been attempted so far"
    )
    created_at: datetime = Field(
        default_factory=datetime.utcnow
    )
    updated_at: datetime = Field(
        default_factory=datetime.utcnow
    )
    # Only populated once task starts running
    started_at: Optional[datetime] = Field(default=None)
    completed_at: Optional[datetime] = Field(default=None)

    class Config:
        populate_by_name = True          # allows both _id and id
        json_encoders = {datetime: lambda dt: dt.isoformat()}



# TODO: handle a case where a task have same name - even worse case, exactly same attributes
class TaskInRedis(BaseModel):
    """
    minimum data about task to be pushed into redis
    send only whats needed to worker
    """
    id: str = Field(
        ...,
        description="mongoDB _id - to be used for unique identification of task , after execution of task is completed"
    )
    task_name   : str
    priority: int
    cron : str
    next_run : datetime
    num_of_retries: int = Field(default = 0)
    max_retries: int
    task_config: Dict[str, Any] = Field(default_factory = dict)
    
    @classmethod
    def from_db(cls, task: TaskInDB) -> "TaskInRedis":
        return cls(
            id = task.id,
            task_name = task.task_name,
            priority= task.priority,
            cron= task.cron,
            next_run= task.next_run,
            num_of_retries= task.num_of_retries,
            max_retries= task.max_retries,
            task_config= task.task_config,
        )
    
    def serialize_for_redis(self) -> str:
        
        return self.model_dump_json()
    
    @classmethod
    def deserialize_from_redis(cls, data: str) -> "TaskInRedis":
        """Worker calls this when it pops from the queue"""
        return cls.model_validate_json(data)

    class Config:
        json_encoders = {datetime: lambda dt: dt.isoformat()}