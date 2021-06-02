from pathlib import Path

import os

from drama.core.annotation import TaskMeta, annotation
from drama.core.docker import DockerRun
from drama.core.model import TempFile
from drama.models.task import TaskResult
from drama.process import Process


# Path to the Python script for the Hello World example.
DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'resources')
SCRIPT = os.path.join(DIR, 'helloworld.py')


@annotation(
    TaskMeta(
        name="HelloWorld",
        desc="Say hello to everyone.",
        outputs=TempFile,
        params=[("greeting", str), ("sleeptime", float)],
    )
)
def execute(pcs: Process, greeting: str, sleeptime: float) -> TaskResult:
    """
    Print greeting for each name in a given input file. Runs code as Python
    script (``resources/helloworld.py``) within a Docker container.
    """
    # Get input file from upstream operator. Expects a temporary file
    # containing the list of names.
    inputs = pcs.get_from_upstream()
    if not inputs:
        raise ValueError("no input file")
    if "TempFile" not in inputs:
        raise ValueError(f"Names file is missing in inputs '{inputs.keys()}'")
    input_file = Path(inputs["TempFile"][0]["resource"])

    # -- Prepare run ----------------------------------------------------------
    # Create base directory for Docker run.
    run = DockerRun(basedir=Path(pcs.storage.local_dir, "helloworld"))
    # Copy Python script to code/helloworld.py in the Docker run folder
    run.copy(src=SCRIPT, dst=os.path.join('code', 'helloworld.py'))
    # Copy names file to data/names.txt in the Docker run folder.
    run.copy(src=input_file, dst=os.path.join('data', 'names.txt'))
    # Create output directory for result file in the Docker run folder.
    run.localpath('results').mkdir()
    # Bind all created directories 'code', 'data', and 'results' as volumes
    # for the Docker container.
    run.bind_dirs()

    # -- Run ------------------------------------------------------------------
    # Run the helloworld.py script using the standard Python Docker image.
    result = run.exec(
        image='python:3.7',
        commands=[
            (
                'python code/helloworld.py '
                '--inputfile data/names.txt '
                '--outputfile results/greetings.txt '
                f'--sleeptime={sleeptime} '
                f'--greeting={greeting}'
            )
        ]
    )

    # -- Process run results --------------------------------------------------
    # Raise error if the run was not successful.
    if not result.is_success():
        raise Exception('\n'.join(result.logs))
    # Add result file to persistent storage and to the step result.
    filepath = run.localpath('results', 'greetings.txt')
    greetings_file = pcs.storage.put_file(filepath)
    output_file = TempFile(resource=greetings_file)
    pcs.to_downstream(output_file)
    return TaskResult(files=[greetings_file])
