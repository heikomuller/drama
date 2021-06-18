"""
This module contains helper functions for executing workflow requests and monitoring the workflow status.
"""

from datetime import datetime
from typing import List, Optional

import time

from drama.manager import TaskManager
from drama.models.task import TaskStatus
from drama.models.workflow import Task, WorkflowRequest
from drama.worker import execute, revoke


def run(
        workflow: WorkflowRequest, poll_interval: Optional[float] = 5.0, verbose: Optional[bool] = False,
        raise_error: Optional[bool] = False
) -> List[Task]:
    """
    Execute and monitor a given workflow request.

    Starts the workflow and continuously polls the workflow state at the given interval. Terminates once the workflow
    execution is complete, indicated either by (i) all tasks being in state **DONE** or (ii) at least one task being
    in an error state.

    If the **verbose** flag is True the current state of the workflow tasks is printed to STDOUT every time the workflow
    state is polled.

    If the **raise_error** flag is True a runtime error is raised if the state of a workflow task is **FAILED**.

    Returns the final list of workflow tasks when the workflow done.

    :param workflow:
    :param poll_interval:
    :param verbose:
    :param raise_error:
    :return:
    """
    # Execute the given workflow request.
    wf = execute(workflow)
    # Monitor workflow state while the workflow is active.
    while True:
        # The poll_interval determines the frequency with which the workflow
        # state is polled
        time.sleep(poll_interval)
        tasks = TaskManager().find({"parent": wf.id})
        # Print status of workflow tasks to STDOUT if the verbose flag is True.
        if verbose:
            status = "".join([f"({t.name}={t.status})" for t in tasks])
            ts = datetime.now().isoformat()[:19]
            print(f"{wf.id}@{ts}: {status}")
        # Check status of all tasks. The workflow is considered done, if
        # (i) one of the tasks failed, or (ii) all tasks are done.
        is_done = True
        for task in tasks:
            if task.status == TaskStatus.STATUS_FAILED:
                # Revoke the workflow to avoid that there are any pending tasks.
                # Make sure that the workflow is not already revoked.
                if "RevokeExecution" not in [t.name for t in tasks]:
                    revoke(wf.id)
                if raise_error:
                    raise RuntimeError(task.result.message)
                break
            elif task.status != TaskStatus.STATUS_DONE:
                is_done = False
        if is_done:
            return tasks
