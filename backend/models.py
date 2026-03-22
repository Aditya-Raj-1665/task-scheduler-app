from pydantic import BaseModel, Field, field_validator, model_validator, ConfigDict
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List
from enum import Enum
from croniter import croniter


class TaskState(str, Enum):
    """All possible states a task can transition through during its lifecycle."""
    CREATED = "CREATED"
    VALIDATING = "VALIDATING"
    INVALID = "INVALID"
    PENDING = "PENDING"
    PAUSED = "PAUSED"
    READY = "READY"
    ACQUIRING_LOCK = "ACQUIRING_LOCK"
    LOCK_FAILED = "LOCK_FAILED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    TIMED_OUT = "TIMED_OUT"
    CANCELLED = "CANCELLED"
    RETRY = "RETRY"
    EXHAUSTED = "EXHAUSTED"


class ExecutionAttempt(BaseModel):
    """Represents a single execution attempt of a task.

    Embedded inside TaskInDB.execution_history to track each run.
    """
    attempt_number: int = Field(..., ge=1)
    started_at: datetime
    ended_at: Optional[datetime] = Field(default=None)
    state: TaskState = Field(..., description="COMPLETED / FAILED / TIMED_OUT")
    fail_reason: Optional[str] = Field(default=None)

    @model_validator(mode="after")
    def validate_attempt(self) -> "ExecutionAttempt":
        """Ensure ended_at is not before started_at, and failure states have a reason."""
        if self.ended_at and self.ended_at < self.started_at:
            raise ValueError("ended_at cannot be before started_at")
        if self.state in (TaskState.FAILED, TaskState.TIMED_OUT) and not self.fail_reason:
            raise ValueError("fail_reason is required when state is FAILED or TIMED_OUT")
        return self


class TaskInput(BaseModel):
    """API input model — what the user sends from the frontend to create a task."""

    task_name: str = Field(
        ...,
        min_length=1,
        max_length=200,
        description="Name of the Task"
    )

    cron: str = Field(
        ...,
        description="Cron expression for scheduling"
    )

    description: Optional[str] = Field(
        default=None,
        max_length=1000,
        description="Description of what the task does"
    )

    start_date: datetime = Field(
        ...,
        description="When the task schedule should start"
    )

    end_date: datetime = Field(
        ...,
        description="When the task scheduling should end"
    )

    priority: int = Field(
        default=3,
        ge=1,
        le=3,
        description="Task priority (1 = highest, 3 = lowest)"
    )

    max_retries: int = Field(
        default=3,
        ge=0,
        le=5,
        description="Number of retries before the task stops scheduling"
    )

    task_config: Dict[str, Any] = Field(
        default_factory=dict,
        description="Specific task configurations (operator_path, payload, connection)"
    )

    timeout_seconds: int = Field(
        default=600,
        ge=1,
        le=3600,
        description="Max seconds a single attempt can run before TIMED_OUT"
    )

    @field_validator("cron")
    @classmethod
    def validate_cron(cls, v: str) -> str:
        """Validate that the cron expression is syntactically correct."""
        if not croniter.is_valid(v):
            raise ValueError(f"Invalid cron expression: '{v}'")
        return v

    @field_validator("start_date", "end_date", mode="before")
    @classmethod
    def ensure_timezone_awareness(cls, v: datetime) -> datetime:
        """Reject naive datetimes — all dates must include timezone info."""
        if isinstance(v, datetime) and v.tzinfo is None:
            raise ValueError("Datetime must be timezone aware")
        return v

    @model_validator(mode="after")
    def end_date_must_be_after_start_date(self) -> "TaskInput":
        """Ensure end_date comes strictly after start_date."""
        if self.end_date <= self.start_date:
            raise ValueError("end_date must be after start_date")
        return self

    model_config = ConfigDict(
        populate_by_name=True,
        json_encoders={datetime: lambda dt: dt.isoformat()}
    )


class TaskInDB(TaskInput):
    """Full task document as stored in MongoDB.

    Extends TaskInput with internal tracking fields like state, next_run,
    retry counts, execution history, and timestamps.
    """
    id: Optional[str] = Field(default=None, alias="_id")
    state: TaskState = Field(default=TaskState.PENDING)

    next_run: Optional[datetime] = Field(
        default=None,
        description="Next scheduled execution time (computed from cron)"
    )
    num_of_retries: int = Field(
        default=0,
        ge=0,
        description="How many retries have been attempted so far"
    )

    execution_history: List[ExecutionAttempt] = Field(default_factory=list)

    retry_after: Optional[datetime] = Field(
        default=None,
        description="Earliest time this task can re-enter the queue after a failure"
    )

    last_run: Optional[datetime] = Field(default=None)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    started_at: Optional[datetime] = Field(default=None)
    completed_at: Optional[datetime] = Field(default=None)
    cancelled_at: Optional[datetime] = Field(default=None)
    paused_at: Optional[datetime] = Field(default=None)

    @model_validator(mode="after")
    def retries_within_bound(self) -> "TaskInDB":
        """Ensure num_of_retries does not exceed max_retries."""
        if self.num_of_retries > self.max_retries:
            raise ValueError("num_of_retries exceeded max_retries")
        return self

    @classmethod
    def from_mongo(cls, doc: dict) -> "TaskInDB":
        """Convert a raw MongoDB document to TaskInDB.

        Handles ObjectId to str conversion for the _id field.

        Args:
            doc: Raw MongoDB document dict.

        Returns:
            TaskInDB instance, or None if doc is None.
        """
        if doc is None:
            return None
        doc = dict(doc)
        if "_id" in doc:
            doc["_id"] = str(doc["_id"])
        for k, v in doc.items():
            if isinstance(v, datetime) and v.tzinfo is None:
                doc[k] = v.replace(tzinfo=timezone.utc)
        return cls(**doc)

    model_config = ConfigDict(
        populate_by_name=True,
        json_encoders={datetime: lambda dt: dt.isoformat()}
    )


class TaskInRedis(BaseModel):
    """Minimal task data pushed into Redis for the worker.

    Contains only the fields the Celery worker needs to execute the task
    and report back results.
    """
    id: str = Field(
        ...,
        description="MongoDB _id used for unique identification after execution"
    )
    task_name: str
    priority: int
    cron: str
    next_run: datetime
    num_of_retries: int = Field(default=0, ge=0)
    max_retries: int

    timeout_seconds: int = Field(
        description="Worker needs this to enforce timeout"
    )

    attempt_number: int = Field(
        default=1,
        ge=1,
        description="Which attempt number to log in execution_history"
    )

    state: TaskState = Field(
        description="Worker checks this before starting to catch mid-queue cancellations"
    )

    task_config: Dict[str, Any] = Field(default_factory=dict)

    @classmethod
    def from_db(cls, task: TaskInDB) -> "TaskInRedis":
        """Create a TaskInRedis instance from a TaskInDB document.

        Args:
            task: Full task document from MongoDB.

        Returns:
            TaskInRedis with only the fields needed by the worker.
        """
        return cls(
            id=task.id,
            task_name=task.task_name,
            priority=task.priority,
            cron=task.cron,
            next_run=task.next_run,
            num_of_retries=task.num_of_retries,
            max_retries=task.max_retries,
            timeout_seconds=task.timeout_seconds,
            attempt_number=len(task.execution_history) + 1,
            state=task.state,
            task_config=task.task_config,
        )

    def serialize_for_redis(self) -> str:
        """Serialize this model to a JSON string for Redis storage.

        Returns:
            JSON string representation of this model.
        """
        return self.model_dump_json()

    @classmethod
    def deserialize_from_redis(cls, data: str) -> "TaskInRedis":
        """Deserialize a JSON string from Redis back into a TaskInRedis instance.

        Args:
            data: JSON string popped from the Redis queue.

        Returns:
            TaskInRedis instance.
        """
        return cls.model_validate_json(data)

    model_config = ConfigDict(
        populate_by_name=True,
        json_encoders={datetime: lambda dt: dt.isoformat()}
    )


class TaskSummary(BaseModel):
    """Lightweight response model for list views (frontend tabs)."""
    id: str
    task_name: str
    state: TaskState
    priority: int
    next_run: Optional[datetime]
    num_of_retries: int
    max_retries: int
    created_at: datetime


class TaskDetail(TaskSummary):
    """Full response model for single task detail view.

    Extends TaskSummary with scheduling context and execution history.
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




# ──────────────────────────────────────────────────────────────
# EXAMPLE TASK  (POST /tasks)
# ──────────────────────────────────────────────────────────────
#
# Full example — every field, with explanations:
#
# {
#   "task_name":        "daily_sales_report",
#   "cron":             "0 2 * * *",               # Every day at 02:00 UTC
#   "description":      "Generates the daily sales summary and emails it to the team.",
#   "start_date":       "2026-01-01T00:00:00+00:00",  # Must be timezone-aware (ISO 8601)
#   "end_date":         "2026-12-31T23:59:59+00:00",  # Must be after start_date
#   "priority":         1,                          # 1 = highest, 3 = lowest (default 3)
#   "max_retries":      3,                          # 0–5 allowed (default 3)
#   "timeout_seconds":  300,                        # 1–3600 seconds (default 600)
#   "task_config": {
#       "operator_path": "operators/example_operator.py",  # Script the worker will execute
#       "payload": {
#           "task_name":   "daily_sales_report",
#           "report_type": "summary",
#           "recipients":  ["team@example.com"]
#       },
#       "connection": {
#           "db_url": "mongodb://localhost:27017",
#           "db_name": "analytics"
#       }
#   }
# }
#
# Minimal example — only the required fields (defaults fill the rest):
#
# {
#   "task_name":  "quick_health_check",
#   "cron":       "*/10 * * * *",
#   "start_date": "2026-03-22T00:00:00+05:30",
#   "end_date":   "2026-06-30T23:59:59+05:30",
#   "task_config": {
#       "operator_path": "operators/example_operator.py",
#       "payload":    { "task_name": "quick_health_check" },
#       "connection": {}
#   }
# }
# ──────────────────────────────────────────────────────────────