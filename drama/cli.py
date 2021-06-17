"""
Simple command-line interface to control drama workflows.
"""

import click
import json

from drama.manager import TaskManager, WorkflowManager
from drama.worker import revoke


# -- Commands -----------------------------------------------------------------

@click.command(name='list')
@click.option(
    '-r',
    '--active',
    default=False,
    is_flag=True,
    help='List only active workflows'
)
def list_workflows(active):
    """List workflows."""
    for wf in WorkflowManager().list_all(active=active):
        click.echo(f"{wf.workflow_id}\t{wf.status}\t{wf.last_update}")


@click.command(name='show')
@click.argument('workflow_id')
def show_workflow(workflow_id):
    """Show workflow information."""
    workflow = WorkflowManager().find_one({"id": workflow_id})
    doc = {
        'workflow_id': workflow_id,
        'created_at': workflow.created_at.isoformat(),
        'is_revoked': workflow.is_revoked,
        'tasks': [task.dict() for task in TaskManager().find({"parent": workflow_id})]
    }
    click.echo(json.dumps(doc, indent=4, default=str))


@click.command(name='revoke')
@click.argument('workflow_id')
def revoke_workflow(workflow_id):
    """Revoke workflow."""
    revoke(workflow_id)


# -- Create command group -----------------------------------------------------

@click.group()
def cli():  # pragma: no cover
    """Command line interface for drama workflows."""
    pass


cli.add_command(list_workflows)
cli.add_command(show_workflow)
cli.add_command(revoke_workflow)
