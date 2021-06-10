"""
Generic execute function for workflow steps that are run using Docker
containers. These workflow steps are implemented by external operators that have
been packaged as Docker images. Internally, the operators are represented as
dictionaries that contain information about the Docker image, input and output
files, additional input parameters, and execution instructions.

The assumption is that the process object provides access to a registry that
contains the specification of registered operators. The generic executor receives
the operation identifier as one of the input arguments and retrieves the
specification for the operator from the registry.

Another assumption is that there are three types of 'storage' available to the
process (i.e., the workflow run). These storages are identified by the following
schema names:

- *store*: The global persistent store for data files that are independent
  of individual workflow runs and that are available as input for tasks in
  different workflows (i.e., the **data catalog**).
- *rundir*: Each workflow run has a dedicated run directory where those files
  are stored that are generated during the workflow run and that are kept
  as the workflow result after the workflow run finishes.
- *context*: Volatile storage for intermediate files that are generated and used
  by different workflow steps. The context represents the upstream/downstream
  flow of data files during workflow execution. The files in the context that
  are not moved to the global store or the workflow run folder will be deleted
  at the end of a successful workflow run.
"""

from pathlib import Path
from string import Template
from typing import Any, Dict, List

from drama.core.docker.registry import OpParameter, DockerOp, InputFile, OutputFile
from drama.core.docker.run import DockerRun
from drama.models.task import TaskResult
from drama.process import Process
from drama.storage.backend.local import LocalResource
from drama.storage.base import Resource


def execute(pcs: Process, op: str, **kwargs) -> TaskResult:
    """
    Generic execute function for workflow steps that are packaged and executed
    as Docker containers.
    """
    # Get the command specification from the command registry. The registry
    # could be part of the process context that is accessible via the process
    # argument.
    task = pcs.catalog.get_op(identifier=op)
    # -- Prepare for Docker run -----------------------------------------------
    # Create base directory for Docker run.
    run = DockerRun(basedir=pcs.storage.tmpdir())
    print(f"run in {run.basedir}")
    # Copy input files listed in the command specification to the temporary
    # run folder.
    for file in task.files.inputs:
        copy_input_file(pcs=pcs, run=run, file=file)
    # Create folders for result files. We need to create these folders in order
    # to be able to bind them as volumes for the Docker container run.
    create_output_folders(run=run, task=task)
    # Dictionary containing arguments for replacing references to variables
    # in command line strings.
    context = dict()
    # Add values for command parameters from the given keyword arguments to the
    # run context.
    for para in task.parameters:
        context[para.name] = get_parameter(parameter=para, arguments=kwargs)
    # Replace references to variables in the command line strings using the
    # values of the run context.
    commands = list()
    for line in task.commands:
        commands.append(Template(line).substitute(context).strip())
    # -- Run the commands in the Docker container -----------------------------
    # The temporary run folder reflects the file structure that is expected
    # by the container commands. By binding all folders in the run directory
    # to the Docker container we make this file structure available inside
    # the container.
    result = run.exec(
        image=task.image,
        commands=commands,
        bind_dirs=True,
        env=task.env
    )
    # -- Persist result files and make them available for downstream tasks ----
    # Raise error if the run was not successful.
    # TODO: Needs a better way to handle output logs.
    if not result.is_success():
        raise Exception('\n'.join(result.logs))
    files = list()
    for file in task.files.outputs:
        resources = handle_result_file(pcs=pcs, run=run, file=file)
        files.extend(resources)
    return TaskResult(files=files)


# -- Helper Functions ---------------------------------------------------------

def copy_input_file(pcs: Process, run: DockerRun, file: InputFile) -> Path:
    """
    Copy an input file into the temporary folder for the Docker run.

    Parameters
    ----------
    pcs: Process
        Process handle.
    run: DockerRun
        Wrapper for Docker run files.
    file: InputFile
        Input file specification.

    Returns
    -------
    Resource
    """
    if file.src.startswith('context::'):
        tag = file.src[len('context::'):]
        # SUGGESTION: Add methods to query upstream resources by type and/or
        # name/tag.
        resource = pcs.find_one_upstream(query=tag)
    elif file.src.startswith('store::'):
        # Load a resource from the global data store.
        name = file.src[len('store::'):]
        resource = pcs.storage.glob.get_file(name)
    elif file.src.startswith('rundir::'):
        # Load a resource from the specific run folder for this workflow run.
        name = file.src[len('rundir::'):]
        resource = pcs.storage.run.get_file(name)
    elif file.src.startswith('url::'):
        # TODO: Not sure if this should be part of a workflow step specification
        # or implemented using a separate ImportFile actor. One could use some
        # form of reasoning over the workflow specification that would modify
        # the workflow to insert such an import step if an url as input is
        # detected and the file has not been downloaded previously.
        pass
    else:
        raise ValueError(f"invalid source file specification '{file.src}'")
    # Raise error if the resource was not found. This could also be part of the
    # new query method.
    if resource is None:
        raise ValueError(f"unknown file '{file.src}'")
    # Copy file to temporary run directory.
    run.copy(src=resource.resource, dst=file.dst)
    return resource


def create_output_folders(run: DockerRun, task: DockerOp):
    """
    Create parent folders for all files that are listed as outputs in the
    Docker command specification.

    Parameters
    ----------
    run: DockerRun
        Wrapper for Docker run files. Contains the reference to the temporary
        run folder where output folders will be created.
    task: DockerOp
        Specification of a workflow step that is executed inside a Docker
        container.
    """
    for file in task.files.outputs:
        parent = Path(run.basedir, file.src).parent
        if not parent.is_dir():
            parent.mkdir(exist_ok=True, parents=True)


def get_parameter(parameter: OpParameter, arguments: Dict) -> Any:
    """
    Get value for a task parameter from user-provided arguments.

    If the parameter value is not included in the list of arguments the defined
    default value is returned.

    The return type is dependent on the parameter type.

    Parameters
    ----------
    parameter: OpParameter
        Declaration for a Docker task parameter that is part of the task
        specification.
    arguments: dict
        Dictionary of user-provided arguments for the task run.

    Returns
    -------
    any
    """
    value = arguments.get(parameter.name, parameter.default)
    # Return None if the value is None.
    if value is None:
        return None
    # Validate the type of the parameter value (or cast).
    # TODO: Handle exceptions for type conversion errors.
    if parameter.type == 'str' and not isinstance(value, str):
        value = str(value)
    elif parameter.type == 'int' and not isinstance(value, int):
        value = int(value)
    elif parameter.type == 'float' and not isinstance(value, float):
        value = float(value)
    else:
        # TODO: Are there any other types (e.g., dates?)
        pass
    # Return value.
    return value


def handle_result_file(pcs: Process, run: DockerRun, file: OutputFile) -> List[Resource]:
    """
    Copy result file to persistent storage and add to downstream results.

    Parameters
    ----------
    pcs: Process
        Process handle.
    run: DockerRun
        Wrapper for Docker run files.
    file: OutputFile
        Output file specification.

    Returns
    -------
    list of Resource
    """
    resources = list()
    filepath = run.localpath(file.src)
    for dst in file.dst:
        if dst.startswith("store::"):
            # TODO: How to specify the target path for the stored file?
            dst = dst[len("store::"):]
            resource = pcs.storage.glob.put_file(src=filepath, dst=dst)
        elif dst.startswith("context::"):
            resource = LocalResource(resource=str(filepath))
            tag = dst[len("context::"):]
            pcs.to_downstream(resource=resource, tag=tag)
        elif dst.startswith("rundir::"):
            dst = dst[len("rundir::"):]
            resource = pcs.storage.run.put_file(src=filepath, dst=dst)
        else:
            raise ValueError(f"invalid destination path '{dst}'")
        resources.append(resource)
    return resources
