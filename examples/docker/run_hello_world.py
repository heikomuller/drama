"""
Example demonstrating the use of the ``drama.core.docker.DockerRun`` helper
class for running workflow steps in Docker containers. Uses the ROB Hello World
workflow as an example. The workflow step reads an input file ``names.txt``
with person names and that writes a greeting for each name in the input file.

The source code for the Hello World example was copied from the GitHub
repository https://github.com/scailfin/rob-demo-hello-world
"""

from pathlib import Path
from typing import Tuple

import argparse
import os
import shutil
import tempfile
import sys

from drama.core.docker import DockerRun

# Path to the Python scripts and input data files for the workflow step.
HELLOWORLD = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'helloworld')


def setup_1(tmpdir: Path) -> Tuple[DockerRun, Path]:
    """
    Create DockerRun with temporary folder for all files that are needed to run
    the example. We then copy the code and data files from the template directory
    to the run directory. In addition, a directory for the results is created and
    all directories are bound to the Docker run to make them available as volumes
    inside the Docker container.
    """
    print('Setup in temporary run folder')
    run = DockerRun(basedir=tmpdir)
    # Copy template files.
    run.copy(src=HELLOWORLD, recursive=True)
    # Create result directory inside the run directory.
    resultdir = run.localpath('helloworld', 'results')
    resultdir.mkdir()
    # Bind code, data, and result folders as volumes for the Docker run.
    run.bind_dirs(pathname='helloworld')
    return run, resultdir


def setup_2(tmpdir: Path) -> Tuple[DockerRun, Path]:
    """
    Use HELLOWORLD as the base directory for the Docker run and create the
    result folder as a temporary folder.
    """
    print('Setup in workflow template folder')
    run = DockerRun(basedir=HELLOWORLD)
    resultdir = tmpdir
    run.bind_dirs().bind(pathname=resultdir, target='results')
    return run, resultdir


def run_scripts(run: DockerRun, script: str, resultdir: Path):
    """
    Run two commands that execute Python scripts using a standard Python
    container image. Print container outputs and the content of the resu;t
    files (in case of a successfull run).
    """
    result = run.exec(
        image='python:3.7',
        commands=[
            (
                f'python code/{script} '
                '--inputfile data/names.txt '
                '--outputfile results/greetings.txt '
                '--sleeptime 1 '
                '--greeting Hello'
            ),
            (
                'python code/analyze.py '
                '--inputfile results/greetings.txt '
                '--outputfile results/analytics.json'
            )
        ]
    )

    # Print outputs to STDOUT and STDERR
    print('\nRun result is {}.\n'.format('ok' if result.is_success() else 'error'))
    if result.logs:
        print('\nConsole outputs from the Docker container:')
        print('\n'.join(result.logs))

    # Print content of output files if the run was successful.
    if result.is_success():
        print('\nContents of run result files.\n')
        print('Greetings:')
        with run.localpath(resultdir, 'greetings.txt').open('rt') as f:
            print('\n'.join([line.strip() for line in f]))
        print('\nAnalytics:')
        import json
        with run.localpath(resultdir, 'analytics.json').open('rt') as f:
            print(json.dumps(json.load(f), indent=4))


if __name__ == '__main__':
    """
    Test for two different setup scenarios:

    - 1: Create a copy of the workflow template files.
    - 2: Run directly on template files.

    The -c, --code option allows to specify the name of the Python script that
    is used to execute the first of the two workflow steps.
    """
    args = sys.argv[1:]

    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--setup", default='1', required=False)
    parser.add_argument("-c", "--code", default='helloworld.py', required=False)

    parsed_args = parser.parse_args(args)
    if parsed_args.setup not in ['1', '2']:
        print(f'Unknown setup {parsed_args.setup} [expected 1 or 2]')
        sys.exit(-1)

    # Create a temporary folder for run files.
    tmpdir = Path(tempfile.mkdtemp())

    if parsed_args.setup == '1':
        run, resultdir = setup_1(tmpdir=tmpdir)
    elif parsed_args.setup == '2':
        run, resultdir = setup_2(tmpdir=tmpdir)

    run_scripts(run=run, script=parsed_args.code, resultdir=resultdir)

    # Remove the temporary directory.
    shutil.rmtree(tmpdir)
