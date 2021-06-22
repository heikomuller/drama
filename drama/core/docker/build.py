"""
This module contains a collection of helper classes to build Python container
images that will be used to execute workflow steps.
"""

from pathlib import Path
from typing import Dict, List

import docker
import os
import shutil
import tempfile

from drama.core.docker.base import Pathname


def py_requirements(requirements: List[str], targetdir: Pathname, dockerfile: List[str]):
    """
    Add lines to a Dockerfile for installing Python requirements.

    Parameters
    ----------
    requirements: list of string
        List of packages that are installed in the Docker image.
    targetdir: Pathname
        Base directory for building the Docker image.
    dockerfile: list of string
        List of statements for the created Dockerfile.

    Returns
    -------
    list of string
    """
    with open(os.path.join(targetdir, "requirements.txt"), "wt") as f:
        for line in requirements:
            f.write(f"{line}\n")
    dockerfile.append("COPY requirements.txt requirements.txt")
    dockerfile.append("RUN pip install -r requirements.txt")


def r_packages(packages: List[Dict], targetdir: Pathname, dockerfile: List[str]):
    """
    Add lines to install packages for a Docker image that is based on the R
    runtime.

    Parameters
    ----------
    packages: list of dict
        List of package specifications
    targetdir: Pathname
        Base directory for building the Docker image.
    dockerfile: list of string
        List of statements for the created Dockerfile.

    Returns
    -------
    string, list of string
    """
    with open(os.path.join(targetdir, "install_packages.R"), "wt") as f:
        for pkg in packages:
            pkg_name = pkg["name"]
            deps = "TRUE" if pkg.get("dependencies", True) else "FALSE"
            repo = pkg.get("repos", "https://cran.r-project.org")
            f.write(f'install.packages("{pkg_name}", dependencies={deps}, repos="{repo}")\n')
    dockerfile.append("COPY install_packages.R install_packages.R")
    dockerfile.append("RUN Rscript install_packages.R")


# -- Generic build for Docker images ------------------------------------------

def docker_build(spec: Dict, sourcedir: Pathname):
    """
    Build Docker image for a given image specification from a drama operator
    specification file.

    The source files for the Docker image are stored in the given ``sourcedir``.

    Parameters
    ----------
    spec: dict
        Docker image specification.
    sourcedir: Pathname
        Path to directory containing input files for the Docker image.
    """
    # Create a temporary folder for the Dockerfile.
    tmpdir = tempfile.mkdtemp()
    # Copy files that are listed in the specification to the temporary folder.
    files = [(f["src"], f["dst"]) for f in spec.get("files", [])]
    for src, dst in files:
        source = Path(sourcedir, src)
        target = Path(tmpdir, src)
        if source.is_file():
            shutil.copy2(src=source, dst=target)
        else:
            shutil.copytree(src=source, dst=target)
    # Copy Dockerfile to the temporary folder. If no Dockerfile is given in the
    # specification we create one depending on the base image.
    with Path(tmpdir, "Dockerfile").open("wt") as f:
        for line in get_dockerfile(spec=spec, sourcedir=sourcedir, targetdir=tmpdir):
            f.write(f"{line}\n")
    # Build the Docker image from the temporary folder and delete the folder
    # afterwards.
    name = spec['tag']
    try:
        # Build Docker image.
        client = docker.from_env()
        image, logs = client.images.build(path=tmpdir, tag=name, nocache=False)
        outputs = [doc["stream"] for doc in logs if doc.get("stream", "").strip()]
        client.close()
        # Return latest tag for created docker image together with the output
        # logs.
        return image.tags[-1], outputs
    finally:
        # Clean up.
        shutil.rmtree(tmpdir)


def get_dockerfile(spec: Dict, sourcedir: Pathname, targetdir: Pathname) -> List[str]:
    """
    Get list of strings representing the Dockerfile for an image that is being
    build.

    Parameters
    ----------
    spec: dict
        Docker image specification.
    sourcedir: Pathname
        Path to directory containing input files for the Docker image.
    targetdir: Pathname
        Base directory for building the Docker image.
    files: list of tuple
        List of files and directories that are copied into the created image.
    """
    dockersrcfile = Path(sourcedir, spec.get("dockerfile", "Dockerfile"))
    if dockersrcfile.is_file():
        with dockersrcfile.open("rt") as f:
            return [line.strip() for line in f]
    else:
        baseimage = spec.get("baseImage")
        lines = [f"FROM {baseimage}"]
        # Add commands for copying files to the created Dockerfile.
        files = [(f["src"], f["dst"]) for f in spec.get("files", [])]
        for src, dst in files:
            lines.append(f"COPY {src} {dst}")
        # Add base image-specific lines to the Dockerfile.
        if "requirements" in spec:
            py_requirements(
                requirements=spec["requirements"],
                targetdir=targetdir,
                dockerfile=lines
            )
        elif "packages" in spec:
            r_packages(
                packages=spec["packages"],
                targetdir=targetdir,
                dockerfile=lines
            )
        return lines
