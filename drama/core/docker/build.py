"""
This module contains a collection of helper classes to build Python container
images that will be used to execute workflow steps.
"""

from pathlib import Path
from typing import List, Optional, Tuple

import docker
import os
import shutil
import tempfile

from drama.core.docker.base import Pathname


# -- Python -------------------------------------------------------------------

def PY_DOCKERFILE(
    basedir: Pathname, requirements: List[str], baseimage: Optional[str] = None,
    files: Optional[List[Tuple[Pathname, str, str]]] = None
):
    """
    Write Dockerfile for the Python container base image.

    TODO: Configure default Python image via environment variable.

    Parameters
    ----------
    basedir: Pathname
        Directory for temporary files.
    requirements: list of string
        List of requirements that will be written to a file ``requirements.txt``
        and installed inside the created Docker image.
    baseimage: string, default=Name
        Name of the base image to use. If no value is given, the default image
        ``python:3.8`` is used. The can also be specified only by one of the
        Python version numbers (3.7, 3.8, 3.9).
    files: list of tuple, default=None
        List of files and folders that need to be copied. Files are specified
        as tuples of source folder, source and target path.

    Returns
    -------
    list of string
    """
    # Set the Python container base image.
    if baseimage in ["3.7", "3.8", "3.9"]:
        baseimage = f"python:{baseimage}"
    elif not baseimage or baseimage == 'python':
        baseimage = "python:3.8"
    # Write Dockerfile to destination path.
    lines = [f'FROM {baseimage}']
    for dirname, src, dst in files if files else []:
        source = Path(dirname, src)
        target = Path(basedir, src)
        if source.is_file():
            shutil.copy2(src=source, dst=target)
        else:
            shutil.copytree(src=source, dst=target)
        lines.append(f'COPY {src} {dst}')
    if requirements:
        lines.append('COPY requirements.txt requirements.txt')
    lines.extend(
        [
            'RUN pip install -r requirements.txt',
        ]
    )
    with Path(basedir, 'Dockerfile').open("wt") as f:
        for line in lines:
            f.write(f"{line}\n")


def docker_build_py(
    name: str, requirements: List[str], baseimage: Optional[str] = None,
    files: Optional[List[Tuple[Pathname, str, str]]] = None
) -> Tuple[str, List[str]]:
    """
    Build a Docker image from a standard Python image with the given
    requirements installed.

    Returns the identifier of the created image.

    Parameters
    ----------
    name: string
        Name for the created image (derived from the workflow step name).
    requirements: list of string
        List of requirements that will be written to a file ``requirements.txt``
        and installed inside the created Docker image.
    baseimage: string, default=Name
        Name of the base image to use. If no value is given, the default image
        ``python:3.8`` is used. The can also be specified only by one of the
        Python version numbers (3.7, 3.8, 3.9).
    files: list of tuple, default=None
        List of files and folders that need to be copied. Files are specified
        as tuples of source and target path.

    Returns
    -------
    string, list of string
    """
    # Create a temporary folder for the Dockerfile.
    tmpdir = tempfile.mkdtemp()
    try:
        # Write requirements.txt to file.
        if requirements:
            with open(os.path.join(tmpdir, 'requirements.txt'), 'wt') as f:
                for line in requirements:
                    f.write(f'{line}\n')
        # Write Dockerfile.
        PY_DOCKERFILE(
            basedir=tmpdir,
            baseimage=baseimage,
            requirements=requirements,
            files=files
        )
        # Build Docker image.
        client = docker.from_env()
        image, logs = client.images.build(path=tmpdir, tag=name, nocache=False)
        outputs = [doc['stream'] for doc in logs if doc.get('stream', '').strip()]
        client.close()
        # Return latest tag for created docker image together with the output
        # logs.
        return image.tags[-1], outputs
    finally:
        # Clean up.
        shutil.rmtree(tmpdir)
