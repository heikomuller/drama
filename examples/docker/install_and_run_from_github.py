"""
This file contains an example that demonstrates the features to register
external (Docker) operators and run a workflow composed of the operators.

It will clone the GitHub repository containing the operator specifications and
then run a workflow that consist of three steps:

1) Download text files with common male and femal first names and combine them
   into a single output file.
2) Take a random sample of names from the downloaded data.
3) Write greeting phrase for each name in the sample file.

The result files of the workflow run are written to a specified output folder.
"""
from pathlib import Path

import argparse
import sys

from drama.core.docker.exec import execute
from drama.core.docker.tests.process import PCS


def main(outputdir: Path, sample_size: int, random_state: int, greeting: str, sleeptime: float):
    # Create fake process object for test purposes. This will clone the GitHub
    # repository and register the operators that are defined in the specification
    # file. All output files will be written to sub-folders 'store' and 'run' in
    # the given output folder.
    pcs = PCS(
        basedir=outputdir,
        sourcedir="https://github.com/heikomuller/docker-helloworld-example.git"
    )
    # Define workflow using the given arguments to parameterize the workflow run.
    workflow = [
        ("DownloadNames", {}),
        ("NamesSample", {"size": sample_size, "randomState": random_state}),
        ("SayHello", {"greeting": greeting, "sleeptime": sleeptime})
    ]
    # Execute the workflow step in sequential order using the generic executor
    # for Docker operators.
    for identifier, kwargs in workflow:
        execute(pcs, op=identifier, **kwargs)
    # Remove temporary folders from the output directory.
    pcs.storage.cleanup()


if __name__ == '__main__':
    args = sys.argv[1:]

    parser = argparse.ArgumentParser()
    parser.add_argument("-g", "--greeting", default='Hello', required=False)
    parser.add_argument("-o", "--outputdir", required=True)
    parser.add_argument("-r", "--rand", required=False)
    parser.add_argument("-s", "--size", default=10, type=int, required=False)
    parser.add_argument("-t", "--sleeptime", default=1.0, type=float, required=False)

    parsed_args = parser.parse_args(args)

    main(
        outputdir=Path(parsed_args.outputdir),
        sample_size=parsed_args.size,
        random_state=parsed_args.rand,
        greeting=parsed_args.greeting,
        sleeptime=parsed_args.sleeptime
    )
