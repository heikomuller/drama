"""
Example demonstrating the use of the ``drama.core.docker.DockerRun`` helper
class for running workflow steps in Docker containers. Uses the ROB Hello World
workflow as an example. The workflow step reads an input file ``names.txt``
with person names and that writes a greeting for each name in the input file.

The source code for the Hello World example was copied from the GitHub
repository https://github.com/scailfin/rob-demo-hello-world
"""

from pathlib import Path

import os
import shutil
import tempfile

from drama.core.docker import DockerRun

# Path to the Python scripts and input data files for the workflow step.
HELLOWORLD = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'helloworld')

# Create a temporary folder for run files.
tmpdir = Path(tempfile.mkdtemp())

# -- Example 1 ----------------------------------------------------------------
#
# Create DockerRun with temporary folder for all files that are needed to run
# the example. We then copy the code and data files from the template directory
# to the run directory. In addition, a directory for the results is created and
# all directories are bound to the Docker run to make them available as volumes
# inside the Docker container.
run = DockerRun(basedir=tmpdir)
# Copy template files.
run.copy(src=HELLOWORLD, recursive=True)
# Create result directory inside the run directory.
resultdir = run.localpath('helloworld', 'results')
resultdir.mkdir()
# Bind code, data, and result folders as volumes for the Docker run.
run.bind_dirs(pathname='helloworld')

# -- Example 2 ----------------------------------------------------------------
# As an alternative: We could use HELLOWORLD as the base directory for the
# Docker run and create the result folder as a temporary folder.
# run = DockerRun(basedir=HELLOWORLD)
# resultdir = tmpdir
# run.bind_dirs().bind(pathname=resultdir, target='results')


# Run two commands that execute Python scripts using a standard Python
# container image.
result = run.exec(
    image='python:3.7',
    commands=[
        (
            'python code/helloworld.py '
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
print('Result {}.\n'.format('ok' if result.is_success() else 'error'))
if result.stdout:
    print('\nConsole:')
    print('\n'.join(result.stdout))
if result.stderr:
    print('\nErrors:')
    print('\n'.join(result.stderr))

# Print content of output files if the run was successful.
if result.is_success():
    print('Greetings:')
    with run.localpath(resultdir, 'greetings.txt').open('rt') as f:
        print('\n'.join([line.strip() for line in f]))
    print('\nAnalytics:')
    import json
    with run.localpath(resultdir, 'analytics.json').open('rt') as f:
        print(json.dumps(json.load(f), indent=4))

# Remove the temporary directory.
shutil.rmtree(tmpdir)
