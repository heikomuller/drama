"""
Registry for workflow operators that are implemented outside of the drama
package and that are executed using Docker containers (referred to as external
operators). The benefit is that these operators can be implemented by the users
of the system using programming languages of their choice. This provides the
ability to have an extensible system where users can contribute their own
operators and (if desired) make them available to other users of the system.

One of the ideas is that the user can package all their code into a single
Git repository together with a specification file (``drama.yaml``) that contains
information about how to invoke the operators and how to package them into
Docker images. The specification file is a file in Yaml format with the following
schema:

.. code-block:: yaml

    operators:
        - name: "unique operator name"
          image: "docker image identifier"
          files:
              inputs:
                  - src: "source file URI"
                    type: "file type identifier"
                    dst: "relative target file path inside Docker container"
              outputs:
                  - src: "relative source file path inside Docker container"
                    type: "file type identifier"
                    dst: "target file URI"
                    tags:
                        - "associate tags with file"
          parameters:
              - name: "unique parameter name"
                type: "data type: str, int, or float"
                default: "optional default value"
          commands:
              - "command that is executed inside the Docker container"
    dockerImages:
        - tag: "tag for the created Docker image"
          baseImage: "tag for the base image (i.e., the runtime)"
          requirements:
              - "Python package references when using python base images"
          files:
              - src: "relative path (inside the repository)"
                dst: "relative path (inside the Docker image)"

The specification contains two main parts: (i) the specifications for the operators,
and (ii) the optional details for building Docker containers that are used to execute
the operators.


Operators
---------

Each operator has a unique ``name`` that is used to identify and reference the operator when
defining a workflow. The ``image`` contains the identifier for the Docker image that will be
used when executing the operator. This image can refer to an existing Docker image or to an
image that is further defined in the ``dockerImages`` part. The ``commands`` element contains
a list of command line statements that will be executed inside the Docker container when the operator
is executed as part of a workflow run. The ``name``, ``image``, and ``commands`` elements are
all mandatory.

The operator specification has two optional elements:

- ``parameters`` define additional input parameters that are expected by the operator when it
is executed. Each parameter has a unique identifier and one of the following raw data types:
str, int, or float. In addition, a default value can be defined for the parameter. The default
value will be used if no value is given for the parameter when the operator is invoked.

- ``files`` contains specifications for input files that will be made available to the Docker
container via mounted volumes, and output files that will be copied from the mounted container
volumes to the workflow context and/or persistent storage. References to files inside a Docker
container are made via relative path expressions. References to files in the workflow context
or on persistent storage are made via tag names or via URIs that have the following
format: <scheme>::<fileIdentifier> The scheme refers to the type of storage that
contains the file. The following two schemes are currently supported:

- *store*: The global persistent store for data files that are independent
  of individual workflow runs and that are available as input for tasks in
  different workflows (i.e., the **data catalog**).
- *rundir*: Each workflow run has a dedicated run directory where those files
  are stored that are generated during the workflow run and that are kept
  as the workflow result after the workflow run finishes.

The *fileIdentifier*  is a relative path expression.


Building Docker Images
----------------------

The ``dockerImages`` element contains a list of directives for building Docker images that
are required for running the defined operators. These images are built from a base image
and can include source code files from the repository. The exact schema for the documents
in the list is dependent on the base image. For Python base images (``python:3.7``, ``python:3.8``,
``python:3.9``) the document can list additional required packages that need to be installed
in the created image. The ``files`` element refers to files in the repository (``src`` is
the file path relative to the repository root) that are copied into the created Docker
image (``dst``).

Docker images will be built once at the time when the operators are registered.
"""

from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import git
import tempfile
import yaml

from drama.core.docker.build import docker_build_py


# -- Operator specifications --------------------------------------------------

@dataclass
class InputFile:
    """
    Input file specification.
    """
    # The source for an input file is specified as a URI with two components,
    # the schema and the file identifier or file path. The file scheme can be
    # one of the following: 'store', 'rundir', or 'context'.
    src: str
    # Not clear if this is needed!
    type: str
    # Relative path specifying the file location inside the Docker container.
    dst: str


@dataclass
class OutputFile:
    """
    Output file specification.
    """
    # Relative path specifying the file location inside the Docker container.
    src: str
    # Not clear if this is needed!
    type: str
    # List of target destinations where the output file will be stored and made
    # available to downstream operators. File destinations are specified as
    # URIs following the same format as input source files.
    dst: Optional[str] = None
    # List of tags for the output file.
    tags: Optional[List[str]] = None


@dataclass
class OperatorFiles:
    """
    Grouping of input and output file specifications for external operators.
    """
    inputs: List[InputFile]
    outputs: List[OutputFile]


@dataclass
class OpParameter:
    """
    Each parameter for an operator has a unique name, a data type (currently one
    of: sr, int, or float), and an optional default value.
    """
    name: str
    type: str
    default: Optional[Any] = None


class DockerOp:
    """
    Wrapper class for a specification of an external operator that is executed
    using a Docker container.
    """
    def __init__(self, doc: Dict):
        """
        Initialize the different attributes and properties fro a given dictionary
        serialization of the operator specification.

        Parameters
        ----------
        doc: dict
            Dictionary serialization of the specification for the operator.
        """
        # The 'name', 'image', and 'commands' are mandatory elements.
        self.name = doc["name"]
        self.image = doc["image"]
        self.commands = doc["commands"]
        # The following elements are optional.
        self.env = doc.get("env", {})
        in_files = doc.get("files", {}).get("inputs", [])
        out_files = doc.get("files", {}).get("outputs", [])
        self.files = OperatorFiles(
            inputs=[
                InputFile(src=obj["src"], type=obj["type"], dst=obj["dst"]) for obj in in_files
            ],
            outputs=[
                OutputFile(src=obj["src"], type=obj["type"], dst=obj.get("dst"), tags=obj.get("tags", [])) for obj in out_files
            ]
        )
        self.parameters = [
            OpParameter(name=obj["name"], type=obj["type"], default=obj.get("default")) for obj in doc.get("parameters", [])
        ]


# -- Operator registry --------------------------------------------------------

class OpRegistry(ABC):
    """
    Interface for the registry of external operators that are executed using
    Docker containers.
    """
    @abstractmethod
    def get_op(self, identifier: str) -> DockerOp:
        """
        Get the operator specification for an external operator that has been
        registered under the given identifier.

        Raises a ValueError if the given identifier is unknown.

        Parameters
        ----------
        identifier: string
            Unique operation identifier

        Returns
        -------
        DockerOp
        """
        pass

    @abstractmethod
    def put_op(self, identifier: str, spec: Dict):
        """
        Add specification for a new operator to the registry.

        Registers the operator under the given identifier. If an operator
        with that identifier already exists, a ValueError is raised.

        The specification ``spec`` contains (i) the lists of input and output files
        for the operator, (ii) the additional (user-provided) parameter values, and
        (iii) the command line statements for running the operator in a Docker container.

        Parameters
        ----------
        identifier: string
            Unique operator identifier.
        spec: dict
            Specification of the operator inputs, outputs, parameters, and
            command line statements for execution within a Docker container.
        """
        pass

    def register(self, source: str, specfile: Optional[str] = "drama.yaml"):
        """
        Register external operators implemented by the files in a given source
        folder, and a specification file.

        The ``source`` either references a folder on the local file system or
        contains the URL of a Git repository. If a repository URL is given,
        the repository will be cloned and the specified operators added to the
        registry.

        If the specification contains the definition of Docker images these
        images will be build locally.

        Parameters
        ----------
        source: str
            Reference to a folder on the local file system or a Git repository.
        specfile: string, default="drama.yaml"
            Relative path to the specification file in the source folder.
        """
        with clone(source) as sourcedir:
            specfilepath = Path(sourcedir, specfile)
            with specfilepath.open("rt") as f:
                doc = yaml.load(f, Loader=yaml.FullLoader)
            # Build any Docker images that are specified in the document.
            for obj in doc.get("dockerImages", []):
                if obj["baseImage"] in ["python:3.7", "python:3.8", "python:3.9"]:
                    docker_build_py(
                        name=obj["tag"],
                        requirements=obj.get("requirements"),
                        baseimage=obj["baseImage"],
                        files=[(sourcedir, f["src"], f["dst"]) for f in obj.get("files", [])]
                    )
            # Add specifications for external operators.
            for obj in doc.get("operators", []):
                self.put_op(identifier=obj['name'], spec=obj)


class VolatileRegistry(OpRegistry):
    """
    Default implementation for an operator registry that maintains all operators
    in a dictionary. This registry does not persist the registered operators.
    """
    def __init__(self, source: Optional[str] = None, specfile: Optional[str] = "drama.yaml"):
        """
        Initialize the registry.

        If a source is given the operators that are defined in the sources'
        specification file will be registered with the initialized registry.

        Parameters
        ----------
        source: str, default=None
            Reference to a folder on the local file system or a Git repository.
        specfile: string, default="drama.yaml"
            Relative path to the specification file in the source folder.
        """
        self._operators = dict()
        if source is not None:
            self.register(source=source, specfile=specfile)

    def get_op(self, identifier: str) -> DockerOp:
        """
        Get the operator specification for an external operator that has been
        registered under the given identifier.

        Raises a ValueError if the given identifier is unknown.

        Parameters
        ----------
        identifier: string
            Unique operation identifier

        Returns
        -------
        DockerOp
        """
        if identifier not in self._operators:
            raise ValueError(f"unknown operator '{identifier}'")
        return self._operators[identifier]

    def put_op(self, identifier: str, spec: Dict):
        """
        Add specification for a new operator to the registry.

        The operator is registered under the given identifier. If an operator
        with that identifier already exists, a ValueError is raised.

        The specification ``spec`` contains the lists of input and output files
        for the operator, the additional (user-provided) parameter values, and
        the command line statements for execution within a Docker container.

        Parameters
        ----------
        identifier: string
            Unique operator identifier.
        spec: dict
            Specification of the operator inputs, outputs, parameters, and
            command line statements for execution within a Docker container.
        """
        if identifier in self._operators:
            raise ValueError(f"operator '{identifier}' exists")
        self._operators[identifier] = DockerOp(doc=spec)


@contextmanager
def clone(source: str) -> str:
    """
    Clone a repository that contains specifications for external operators that
    are added to the registry.

    If source points to a directory on local disk it is returned as the *cloned*
    source directory. Otherwise, it is assumed that source points to a Git
    repository. The repository is cloned into a temporary directory which is
    removed when the generator resumes after the specifications have been added
    to the local repository.

    Returns the path to the resulting source folder on the local disk that
    contains the *cloned* repository files.

    Parameters
    ----------
    source: string
        The source is either a path to local directory, or the URL for a Git
        repository.

    Returns
    -------
    string
    """
    if Path(source).is_dir():
        # Return the source if it references a directory on local disk.
        yield source
    else:
        sourcedir = tempfile.mkdtemp()
        print('cloning into {}'.format(sourcedir))
        try:
            git.Repo.clone_from(source, sourcedir)
            yield sourcedir
        except (IOError, OSError, git.exc.GitCommandError) as ex:
            raise ex
        finally:
            # Make sure to cleanup by removing the created temporary folder.
            # Avoid permission errors for read-only files on Windows based on
            # https://stackoverflow.com/questions/58878089
            git.rmtree(sourcedir)
