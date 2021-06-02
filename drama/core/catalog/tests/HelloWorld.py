from pathlib import Path

import os

from drama.core.annotation import TaskMeta, annotation
from drama.core.docker import DockerRun
from drama.core.model import TempFile
from drama.models.task import TaskResult
from drama.process import Process


# Path to the Python script for the Hello World example.
DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'resources')
NAMES = os.path.join(DIR, 'NAMES-F.txt')
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
    """
    inputs = pcs.get_from_upstream()
    if not inputs:
        raise ValueError("no input file")

    # Expects an excel dataset in the inputs. Raise error if the 'ExcelDataset' key
    # is missing in the inputs dictionary.
    if "TempFile" not in inputs:
        raise ValueError(f"Names file is missing in inputs '{inputs.keys()}'")

    input_file = Path(inputs["TempFile"][0]["resource"])

    run = DockerRun(basedir=Path(pcs.storage.local_dir, "helloworld"))
    run.copy(src=SCRIPT, dst=os.path.join('code', 'helloworld.py'))
    run.copy(src=input_file, dst=os.path.join('data', 'names.txt'))
    run.localpath('results').mkdir()
    run.bind_dirs()

    result = run.exec(
        image='python:3.7',
        commands=[
            (
                'python code/helloworld.py '
                '--inputfile data/names.txt '
                '--outputfile results/greetings.txt '
                '--sleeptime 1 '
                '--greeting Hello'
            )
        ]
    )

    if not result.is_success():
        raise Exception('\n'.join(result.logs))

    filepath = run.localpath('results', 'greetings.txt')
    # Send to remote storage
    greetings_file = pcs.storage.put_file(filepath)
    # Send to downstream
    output_file = TempFile(resource=greetings_file)
    pcs.to_downstream(output_file)
    return TaskResult(files=[greetings_file])
