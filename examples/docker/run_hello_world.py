"""
Example demonstrating the use of the ``drama.core.docker.DockerRun`` helper
class for running workflow steps in Docker containers. Uses a variation of the
REANA Hello World workflow as an example. The workflow operators are defined in
the GitHub repository: https://github.com/heikomuller/docker-helloworld-example

Before running the workflow, the operators need to be registered with the
local database:

poetry run drama register -s https://github.com/heikomuller/docker-helloworld-example.git
"""

import argparse
import sys

from drama.models.task import TaskRequest
from drama.models.workflow import WorkflowRequest
from drama.worker import run


def main(gender: str, sample_size: int, random_state: int, greeting: str, sleeptime: float):
    """
    Run the Hello World workflow using the generic executor for workflow
    operators that are executed using Docker.
    """
    # Define the three-step workflow that (i) downloads the names file, (ii)
    # takes a random sample of names, and (iii) prints a greeting phrase for
    # each name in the sample. The operator for the first step depends on the
    # value for the gender parameter.
    if gender == "female":
        download_op = "DownloadFemaleNames"
    elif gender == "male":
        download_op = "DownloadMaleNames"
    else:
        download_op = "DownloadAllNames"
    task_download = TaskRequest(
        name="DownloadNames",
        module="drama.core.docker.exec",
        params={"op": f"demo.hello-world.{download_op}"}
    )
    task_sample = TaskRequest(
        name="SampleNames",
        module="drama.core.docker.exec",
        params={
            "op": "demo.hello-world.SampleNames",
            "size": sample_size,
            "randomState": random_state
        },
        inputs={"namesFile": "DownloadNames.FileResource#namesFile"}
    )
    task_say_hello = TaskRequest(
        name="SayHello",
        module="drama.core.docker.exec",
        params={
            "op": "demo.hello-world.SayHello",
            "greeting": greeting,
            "sleeptime": sleeptime
        },
        inputs={"namesFile": "SampleNames.FileResource#namesSampleFile"}
    )
    workflow = WorkflowRequest(
        tasks=[
            task_download,
            task_sample,
            task_say_hello
        ]
    )
    # Execute and monitor the workflow run.
    tasks = run(workflow, verbose=True, raise_error=True)
    print(tasks)


if __name__ == '__main__':
    args = sys.argv[1:]

    parser = argparse.ArgumentParser()
    parser.add_argument("-g", "--greeting", default='Hello', required=False)
    parser.add_argument("-r", "--rand", type=int, required=False)
    parser.add_argument("-s", "--size", default=10, type=int, required=False)
    parser.add_argument("-t", "--sleeptime", default=1.0, type=float, required=False)
    parser.add_argument("-x", "--gender", required=False)

    parsed_args = parser.parse_args(args)

    main(
        sample_size=parsed_args.size,
        random_state=parsed_args.rand,
        greeting=parsed_args.greeting,
        sleeptime=parsed_args.sleeptime,
        gender=parsed_args.gender
    )
