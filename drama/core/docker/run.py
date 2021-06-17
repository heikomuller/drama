"""
This module contains a collection of helper classes for executing workflow
steps that run inside Docker containers. The aim is to reduce duplication
of code and to hide some of the complexity by providing helper methods for
common tasks.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from docker.errors import ContainerError, ImageNotFound, APIError
from pathlib import Path
from typing import Dict, List, Optional, Union

import docker
import shutil

from drama.core.docker.base import Pathname, stacktrace
from drama.core.docker.registry import ContainerRegistry


@dataclass
class ExecResult:
    """
    Result of executing a (list of) command(s) inside a Docker container.

    Contains  a return code to signal success (=0) or error (<>0).

    Outputs that were written to standard output and standard error are
    captured in the log as a list of strings.

    If an exception was raised during execution it is captured in the
    respective property *exception*.
    """
    returncode: Optional[int] = 0
    logs: Optional[List[str]] = field(default_factory=list)
    exception: Optional[Exception] = None

    def is_error(self) -> bool:
        """
        Test if the return code is non-zero indicating an error run.

        Returns
        -------
        int
        """
        return self.returncode != 0

    def is_success(self) -> bool:
        """
        Test if the return code is zero indicating a successful run.

        Returns
        -------
        int
        """
        return self.returncode == 0


class DockerRun:
    """
    Wrapper for methods to prepare and execute one or more commands inside a
    Docker container. The class maintains (i) a reference to a folder on the
    local storage that is used to prepare files for a Docker run, and (ii)
    bindings of folders on the local store to volumes that are mounted inside
    the Docker container for the Docker run.
    """
    def __init__(self, basedir: Pathname, create: Optional[bool] = True):
        """
        Initialize the base directory that is used to store files that are
        required for the Docker run.

        If the *create* flag is *True* the base directory (and all of its
        parent directories) will be created (if they don't exist). If the flag
        is *False* an error is raised if the base directory does not exist.

        Parameters
        ----------
        basedir: Pathname
            Relative base directory in the process' local storage that is used
            to store files for the Docker run.
        create: bool, default=True
            if *True*, create the *basedir* if it does not exist.
        """
        self.basedir = Path(basedir).absolute()
        # Create the basedir if it does not exists (create=True) or raise a
        # ValueError (create=False).
        if not self.basedir.is_dir():
            if create:
                # Use exist_ok=True to avoid issues with race conditions.
                self.basedir.mkdir(parents=True, exist_ok=True)
            else:
                raise ValueError(f"'{basedir}' is not a directory")
        # Bindings for folders in the local store of the Process that will be
        # mounted as volumes for the Docker container.
        self._volumes = dict()

    def bind(
        self, pathname: Pathname, target: str, create: Optional[bool] = True
    ) -> DockerRun:
        """
        Bind the folder with the given path name as a target volume for the
        Docker container when executing the Docker run.

        If the path name is a relative path it is assumed to reference a
        folder that is relative to the base directory of the run.

        If the *pathname* does not reference an existing folder it is either
        created (if the *create* flag is *True*) or an error is raised.

        Returns a reference to the object itself to allow chaining of methods.

        Parameters
        ----------
        pathname: Pathname
            Absolute or relative path to a folder on the local file store.
        target: string
            Target path for the mounted volume in the Docker container.
        create: bool, default=True
            Create directory if the *pathname* folder does not exist.

        Returns
        -------
        DockerRun
        """
        path = Path(pathname)
        # Create path that is relative to the run base directory if the given
        # path expression is not an absolute path.
        if not path.is_absolute():
            path = Path(self.basedir, path)
        # Raise error if path does not reference a directory and the raise_error
        # flag is True.
        if not path.is_dir():
            if create:
                path.mkdir(parents=True, exist_ok=True)
            else:
                raise ValueError(f"'{pathname}' is not a directory")
        self._volumes[path] = target
        return self

    def bind_base(self, target: str) -> DockerRun:
        """
        Bind base directory of the Docker run under the given target path.

        Parameters
        ----------
        target: string
            Target path for the mounted volume in the Docker container.

        Returns
        -------
        DockerRun
        """
        self._volumes[self.basedir] = target
        return self

    def bind_dirs(self, pathname: Optional[Pathname] = None) -> DockerRun:
        """
        Bind all folders in the given directory for the Docker run by their
        name as volumes for the Docker run.

        If no path is given the base directory will be used as the source path.

        The created binding is a 1:1-mapping of folders from the given directory
        to the folder structure in the Docker container. This binding is intended
        for those use-cases where a single directory is setup to reflect the
        directory structure that is expected by the Docker container.

        Returns
        -------
        DockerRun
        """
        if pathname:
            path = Path(pathname)
            if not path.is_absolute():
                path = Path(self.basedir, path)
        else:
            path = self.basedir
        for f in path.glob('*'):
            if f.is_dir():
                self._volumes[f] = f.name
        return self

    def clear(self, erase_files: Optional[bool] = False) -> DockerRun:
        """
        Clear the defined bindings.

        If the *erase_files* flag is True all files and folders in the base
        directory will be deleted.

        Parameters
        ----------
        erase_files: bool, default=False
            Delete all files in the Docker run base directory if True.

        Returns
        -------
        DockerRun
        """
        # Clear existing bindings.
        self._volumes = dict()
        # Remove files and folders in the base directory if the erase_files
        # flag is True.
        if erase_files:
            for f in self.basedir.glob('*'):
                if f.is_file():
                    f.unlink()
                else:
                    shutil.rmtree(f)
        return self

    def copy(self, src: Pathname, dst: Optional[str] = None, recursive: Optional[bool] = False):
        """
        Copy a file or folder to the relative target path (*dst*) in the
        base directory for the Docker run.

        By default, only files will be copied. For copying directories, the
        *recursive* flag has to be set to *True*. If the given source
        references a directory and the *recursive* flag is set to *False*,
        a ValueError will be raised.

        If the target path is not given files will be copied into the base
        directory for the Docker run under the same name as the source path.

        Parameters
        ----------
        src: Pathname
            Source path for the copied file or folder.
        dst: string, default=None
            Relative target path for the copied file or folder.
        recursive: bool, default=False
            Recursively copy directories.
        """
        # Ensure that the parent directory for the target exists.
        dst = dst if dst else Path(src).name
        target = self.localpath(dst)
        target.absolute().parent.mkdir(parents=True, exist_ok=True)
        source = Path(src)
        if source.is_file():
            shutil.copy2(src=source, dst=target)
        elif recursive:
            shutil.copytree(src=source, dst=target)
        else:
            raise ValueError(f"recursive not specified for directory '{src}'")

    def erase(self):
        """
        Delete the base directory.

        After erasing the base directory other methods may fail due to the
        missing directory. This method should only be called at the end of
        a Docker run for cleanup purposes.
        """
        shutil.rmtree(self.basedir)

    def exec(
        self, image: str, commands: Union[str, List[str]], env: Optional[Dict] = None,
        bind_dirs: Optional[bool] = False, remove: Optional[bool] = True,
        workflow_id: Optional[str] = None, registry: Optional[ContainerRegistry] = None
    ) -> ExecResult:
        """
        Execute one or more commands in a Docker container for the prepared
        Docker run.

        Returns a result object with the return code and logs for the executed
        container.

        Parameters
        ----------
        image: string
            Identifier of the Docker image to run.
        commands: string or list of string
            Command(s) that are executed inside the Docker container.
        env: dict, default=None
            Optional mapping of environment variables that are passed to the
            Docker container.
        bind_dirs: bool, default=False
            Bind all folders in the Docker run base directory prior to running
            the container. Equivalent to calling ``bind_dirs()``.
        remove: bool, default=True
            Remove Docker container after it finished running.
        workflow_id: str, default=None
            Optional identifier for the workflow if the container is executed as
            a task in that workflow.
        registry: ContainerRegistry, default=None
            Registry for running containers. Only set if the container runs
            inside a workflow task. If given, the workflow identifier is
            expected to be not None.

        Returns
        -------
        ExecResult
        """
        # Bind directories if the bind_dirs flag is set.
        if bind_dirs:
            self.bind_dirs()
        # Ensure that commands is a list.
        commands = commands if isinstance(commands, list) else [commands]
        # Create bindings for defined volumes.
        volumes = {f.absolute(): {'bind': f'/{target}', 'mode': 'rw'} for f, target in self._volumes.items()}
        # Run the individual commands using the local Docker daemon.
        result = ExecResult()
        client = docker.from_env()
        try:
            for cmd in commands:
                # Run detached container to be able to capture output to
                # both, STDOUT and STDERR. DO NOT remove the container yet
                # in order to be able to get the captured outputs.
                container = client.containers.run(
                    image=image,
                    command=cmd,
                    volumes=volumes,
                    remove=False,
                    environment=env,
                    detach=True
                )
                # Add container identifier to the registry for running Docker
                # containers.
                if registry:
                    registry.insert(workflow=workflow_id, container=container.id)
                # Wait for container to finish. The returned dictionary will
                # contain the container's exit code ('StatusCode').
                r = container.wait()
                # Remove containe registry entry.
                if registry:
                    registry.remove(workflow=workflow_id, container=container.id)
                # Add container logs to the logs for the Docker run.
                logs = container.logs()
                if logs:
                    result.logs.append(logs.decode('utf-8'))
                # Remove container if the remove flag is set to True.
                if remove:
                    container.remove()
                # Check exit code for the container. If the code is not zero
                # an error occurred and we exit the commands loop.
                status_code = r.get('StatusCode')
                if status_code != 0:
                    result.returncode = status_code
                    break
        except (ContainerError, ImageNotFound, APIError) as ex:
            strace = '\n'.join(stacktrace(ex))
            result.logs.append(strace)
            result.exception = ex
            result.returncode = 1
        client.close()
        return result

    def localpath(self, *args) -> Path:
        """
        Create a path object for a file or folder that is relative to the base
        directory for the Docker run.

        Parameters
        ----------
        args: list of Pathname
            Path expression for file or directory that is relative to the
            base directory for the Docker run.

        Returns
        -------
        Path
        """
        return Path(self.basedir, *args)
