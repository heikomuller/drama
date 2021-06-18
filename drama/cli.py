"""
Simple command-line interface to control drama workflows.
"""

import click
import json
import yaml as yml

from drama.core.docker.registry import PersistentRegistry
from drama.manager import TaskManager, WorkflowManager
from drama.worker import revoke


# -- Operator registry --------------------------------------------------------

@click.command(name="install")
@click.option(
    "-r", "--replace",
    default=False,
    is_flag=True,
    help="Replace existing operators"
)
@click.option(
    "-s", "--source",
    required=True
)
@click.option(
    "-f", "--specfile",
    default="drama.yaml",
    required=False
)
def register_operators(source, specfile, replace):
    """Install workflow operators from repository."""
    # CLI command for registering new Docker operators from a source
    # directory or GitHub repository.
    ops = PersistentRegistry().register(
        source=source,
        specfile=specfile,
        replace=replace
    )
    click.echo("\nSuccessfully registered the following operators:")
    for op_id in ops:
        click.echo(f'- {op_id}')


@click.command(name="list")
def list_operators():
    """List installed workflow operators."""
    ops = PersistentRegistry().list_ops()
    ops = sorted(ops, key=lambda op: op[0])
    click.echo("\nThe following operators are currently installed:")
    for op_id, op in ops:
        click.echo(f'{op_id} ({op.version})')


@click.command(name="show")
@click.option(
    '-y',
    '--yaml',
    default=False,
    is_flag=True,
    help='Show in YAML format'
)
@click.argument("operator")
def show_operator(operator, yaml):
    """Print operator specification."""
    op = PersistentRegistry().get_op(operator)
    if yaml:
        click.echo(yml.dump(op.to_dict()))
    else:
        click.echo(json.dumps(op.to_dict(), indent=4, default=str))


@click.group(name="pm")
def cli_pm():
    """Drama package manager."""
    pass


cli_pm.add_command(register_operators)
cli_pm.add_command(list_operators)
cli_pm.add_command(show_operator)


# -- Workflow Commands --------------------------------------------------------

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
@click.option(
    '-y',
    '--yaml',
    default=False,
    is_flag=True,
    help='Show in YAML format'
)
def show_workflow(workflow_id, yaml):
    """Show workflow information."""
    workflow = WorkflowManager().find_one({"id": workflow_id})
    doc = {
        'workflow_id': workflow_id,
        'created_at': workflow.created_at.isoformat(),
        'is_revoked': workflow.is_revoked,
        'tasks': [task.dict() for task in TaskManager().find({"parent": workflow_id})]
    }
    if yaml:
        click.echo(yml.dump(doc, default_flow_style=False))
    else:
        click.echo(json.dumps(doc, indent=4, default=str))


@click.command(name='revoke')
@click.argument('workflow_id')
def revoke_workflow(workflow_id):
    """Revoke workflow."""
    revoke(workflow_id)


@click.group(name="workflows")
def cli_workflows():
    """Drama workflow manager."""
    pass


cli_workflows.add_command(list_workflows)
cli_workflows.add_command(show_workflow)
cli_workflows.add_command(revoke_workflow)


# -- Create command group -----------------------------------------------------

@click.group()
def cli():
    """Command line interface for drama workflows."""
    pass


cli.add_command(cli_pm)
cli.add_command(cli_workflows)
