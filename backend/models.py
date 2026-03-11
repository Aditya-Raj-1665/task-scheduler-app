from pydantic import BaseModel, Field, field_validator, model_validator,ConfigDict
from datetime import datetime,timezone
from typing import Dict, Any, Optional, List
from enum import Enum
from croniter import croniter

class TaskState(str,Enum):
    """
    All the possible states a task can go through
    """
    CREATED = "CREATED"                     #user just added it
    VALIDATING = "VALIDATING"               #being validated
    INVALID = "INVALID"                     #validation failed
    PENDING = "PENDING"                     #sitting in DB1(schedules table)
    PAUSED = "PAUSED"                       #user paused it
    READY = "READY"                         #in DB2 (queue_table)
    ACQUIRING_LOCK = "ACQUIRING_LOCK"       #about to run, so getting lock
    LOCK_FAILED = "LOCK_FAILED"             #failed to get lock
    RUNNING = "RUNNING"                     #currently executing
    COMPLETED = "COMPLETED"                 #success
    FAILED = "FAILED"                       #for retry mechanism
    TIMED_OUT = "TIMED_OUT"                 #exceeded running time, so timeout
    CANCELLED = "CANCELLED"                 #user cancelled
    RETRY = "RETRY"                         #waiting to retry
    EXHAUSTED = "EXHAUSTED"                 #no retries left


class ExecutionAttempt(BaseModel):
    """
    Represents a single execution attempt of a task.
    Embedded inside TaskInDB.execution_history.
    """
    attempt_number : int = Field(..., ge=1)
    started_at: datetime
    ended_at : Optional[datetime] = Field(default=None)
    state: TaskState = Field(..., description="COMPLETED / FAILED / TIMED_OUT")
    fail_reason : Optional[str] = Field(default = None)
    
    @model_validator(mode="after")
    def validate_attempt(self) -> "ExecutionAttempt":
        if self.ended_at and self.ended_at < self.started_at:
            raise ValueError("ended_at cannot be before started_at")
        if self.state in (TaskState.FAILED, TaskState.TIMED_OUT) and not self.fail_reason:
            raise ValueError("fail_reason is required when state is FAILED or TIMED_OUT")
        return self
    
# user input task model
class TaskInput(BaseModel):
    """
    What the user sends from the frontend to create a task.
    This is the API input model.
    """
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
        description="description of the task. OR what the Task does?"
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
        default_factory = dict,                                     #default_factory?
        description="specific task configurations"
    )
    
    timeout_seconds: int = Field(
        default=600,
        ge=1,
        le=3600,  # 1 hour hard ceiling
        description="Max seconds a single attempt can run before TIMED_OUT"
    )
    
    @field_validator("cron")
    @classmethod
    def validate_cron(cls, v: str) -> str:
        if not croniter.is_valid(v):
            raise ValueError(f"Invalid cron expression: '{v}'")
        return v
    
    @field_validator("start_date","end_date", mode= "before")       #mode = before??
    @classmethod
    def ensure_timezone_awareness(cls , v: datetime) -> datetime:
        if isinstance(v, datetime) and v.tzinfo is None:
            raise ValueError(f"Datetime must be timezone aware")
        return v
    
    @model_validator(mode = "after")                                #mode = "after" ??
    def end_date_must_be_after_start_date(self) -> "TaskInput":
        if self.end_date <=self.start_date:
            raise ValueError("end_date must be after start_date")
        return self

    model_config = ConfigDict(
        populate_by_name=True,
        json_encoders={datetime: lambda dt: dt.isoformat()}
    )

class TaskInDB(TaskInput):
    """
    Full task document as stored in MongoDB.
    Extends TaskSchedule with all internal tracking fields.
    """
    id: Optional[str] = Field(
        default=None, alias="_id"
    )

    state: TaskState = Field(
        default=TaskState.PENDING
    )
    
    next_run: Optional[datetime] = Field(
        default=None,
        description="Next scheduled execution time (computed from cron)"
    )
    num_of_retries: int = Field(
        default=0,
        ge=0,
        description="How many retries have been attempted so far"
    )
    
    execution_history : List[ExecutionAttempt] = Field(
        default_factory= list
    )
    
    retry_after: Optional[datetime] = Field(
        default=None,
        description="Earliest time this task is eligible to re-enter the queue after a failure"
    )
    
    last_run: Optional[datetime] = Field(
        default=None
    )

    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    updated_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    
    started_at: Optional[datetime] = Field(default=None)
    completed_at: Optional[datetime] = Field(default=None)
    cancelled_at: Optional[datetime] = Field(default=None)
    paused_at: Optional[datetime] = Field(default=None)
    
    @model_validator(mode = "after")
    def retries_within_bound(self) -> "TaskInDB":
        if self.num_of_retries > self.max_retries:
            raise ValueError("num_of_retries exceeded max_retries")
        return self
    
    @classmethod
    def from_mongo(cls , doc: dict) ->"TaskInDB":
        """
        Converts a raw MongoDB document to TaskInDB.
        Handles ObjectId → str conversion cleanly.
        Used especially for _id.
        """   
        if doc is None:
            return None
        doc=dict(doc)
        if "_id" in doc:
            doc["_id"] = str(doc["_id"])
        return cls(**doc)

    model_config = ConfigDict(
        populate_by_name=True,
        json_encoders={datetime: lambda dt: dt.isoformat()}
    )

 
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
    num_of_retries: int = Field(default = 0, ge = 0)
    max_retries: int
    
    timeout_seconds: int = Field(
        description="worker needs this to enforce timeout itself"
    )

    attempt_number: int = Field(
        default=1, 
        ge=1 , 
        description="so worker knows which attempt to log in execution_history"
    )

    state: TaskState = Field(
        description="worker checks this before starting — catches mid-queue cancellations"
    )

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
            timeout_seconds=task.timeout_seconds,              
            attempt_number=len(task.execution_history) + 1, 
            state=task.state, 
            task_config= task.task_config,
        )
    
    def serialize_for_redis(self) -> str:
        return self.model_dump_json()
    
    @classmethod
    def deserialize_from_redis(cls, data: str) -> "TaskInRedis":
        """Worker calls this when it pops from the queue"""
        return cls.model_validate_json(data)
    
    model_config = ConfigDict(
        populate_by_name=True,
        json_encoders={datetime: lambda dt: dt.isoformat()}
    )

'''
Below 2 models are for frontend , which will be reponse from the system to the user.
'''
class TaskSummary(BaseModel):
    """
    Lightweight model for list views (the 3 frontend tabs).
    """
    id: str
    task_name: str
    state: TaskState
    priority: int
    next_run: Optional[datetime]
    num_of_retries: int
    max_retries: int
    created_at: datetime

class TaskDetail(TaskSummary):
    """
    Full model for single task view.
    Extends TaskSummary with history and scheduling context.
    """
    description: Optional[str]
    cron: str
    start_date: datetime
    end_date: datetime
    timeout_seconds: int
    last_run: Optional[datetime]
    retry_after: Optional[datetime]
    started_at: Optional[datetime]
    completed_at: Optional[datetime]
    cancelled_at: Optional[datetime]
    paused_at: Optional[datetime]
    execution_history: List[ExecutionAttempt]

# TODO : write a better example task here. 
# EXAMPLE TASK
# {
#   "task_name": "my_first_task",
#   "priority": 1,
#   "start_date": "2025-01-01T00:00:00",
#   "end_date":   "2025-12-31T00:00:00",
#   "task_config": {
#     "operator_path":    "operators/example_operator.py",
#     "payload":          { "task_name": "my_first_task" },
#     "connection":       { "db_url": "mongodb://localhost:27017" },
#     "timeout_seconds":  600
#   }
# }