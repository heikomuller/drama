from abc import ABC
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Set

from pymongo.database import Database

from drama.database import get_db_connection
from drama.models.task import TaskStatus
from drama.models.workflow import Task, Workflow


@dataclass
class WorkflowDescriptor:
    workflow_id: str
    status: str
    last_update: datetime


class BaseManager(ABC):
    """
    Base manager with database connection.
    """

    def __init__(self, db: Optional[Database] = None):
        self.database = db or get_db_connection()


class TaskManager(BaseManager):
    def find(self, query: dict):
        """
        Get task(s) from database based on `query`.
        """
        tasks: dict = self.database.task.find(query)
        tasks_in_db = []

        for task in tasks:
            tasks_in_db.append(Task(**task))

        return tasks_in_db

    def create_or_update_from_id(self, task_id: str, **extra_fields) -> Task:
        """
        Create (or update with `extra_fields`) task from database based on unique `task_id`.
        """
        task = Task(id=task_id, **extra_fields)

        # if the record does not exist, insert it
        self.database.task.update(
            {"id": task_id},
            {"$set": task.dict(exclude_unset=True)},
            upsert=True,
        )

        return task


class WorkflowManager(BaseManager):
    def find_one(self, query: dict) -> Optional[Workflow]:
        """
        Get workflow from database based on `query`.
        """
        workflow_in_db: dict = self.database.workflow.find_one(query)

        if workflow_in_db:
            return Workflow(**workflow_in_db)

        return None

    def create_or_update_from_id(self, workflow_id: str, **extra_fields) -> Workflow:
        """
        Create (or update with `extra_fields`) workflow from database based on unique `workflow_id`.
        """
        workflow = Workflow(id=workflow_id, **extra_fields)

        # if the record does not exist, insert it
        self.database.workflow.update(
            {"id": workflow_id},
            {"$set": workflow.dict(exclude_unset=True)},
            upsert=True,
        )

        return workflow

    def list_all(self, active: Optional[bool] = False) -> List[WorkflowDescriptor]:
        """
        Get a listing of all workflows in the database.

        Returns a list of tuples containing the workflow identifier and the
        workflow state. The state is derived from the state of the workflow
        tasks.

        If all tasks are in the same state that state is returned. Otherwise,
        if one task is in state FAILED, the resulting state is FAILED. If no
        task is in failed state the result is RUNNING.
        """
        workflows = defaultdict(list)
        select_clause = {"_id": 0, "parent": 1, "status": 1, "updated_at": 1}
        for doc in self.database.task.find({}, select_clause):
            workflow_id = doc['parent']
            workflows[workflow_id].append((doc['status'], doc['updated_at']))
        result = list()
        for key, value in workflows.items():
            wf = WorkflowDescriptor(
                workflow_id=key,
                status=get_status({st for st, _ in value}),
                last_update=max([ts for _, ts in value])
            )
            if not active or wf.status == TaskStatus.STATUS_RUNNING:
                result.append(wf)
        return result


# -- Helper Functions

def get_status(values: Set) -> str:
    """
    Get status of a workflow from the set of states of the workflow tasks.

    If all tasks are in the same state that state is returned. Otherwise,
    if one task is in state FAILED, the resulting state is FAILED. If no
    task is in failed state the result is RUNNING.
    """
    if len(values) == 1:
        return next(iter(values))
    elif TaskStatus.STATUS_FAILED in values:
        return TaskStatus.STATUS_FAILED
    else:
        return TaskStatus.STATUS_RUNNING
