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
                  - src: "source file label"
                    dst: "relative target file path inside Docker container"
              outputs:
                  - src: "relative source file path inside Docker container"
                    datatype:
                        - uri: "Data type URI"
                    label: "output file label"
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
          packages:
              - name: "Name of packages whose current versions should be downloaded"
                dependencies: "logical indicating whether to also install uninstalled packages which these packages depend on"
                repos": "base URL of the repositorie to use,"
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
volumes to the workflow context. References to files inside a Docker container are made via
relative path expressions. References to files in the workflow context are made via labels that
are assigned to the files as path of the inputs specification of a TaskRequest.


Building Docker Images
----------------------

The ``dockerImages`` element contains a list of directives for building Docker images that
are required for running the defined operators. These images are built from a base image
and can include source code files from the repository. The exact schema for the documents
in the list is dependent on the base image.

For Python base images (``python:3.7``, ``python:3.8``, ``python:3.9``) the document can
list additional required packages (using the ``requirements`` element) that need to be
installed in the created image.

For R base images (``rocker/r-ver``, ``rocker/tidyverse``, ``rocker/verse``) the document
can list additional packages (using the ``packages`` element) that need to be
installed in the created image. The file ``install_packages.R`` is reserved for
the build command and should not be specified in the ``files`` element of the build
specification. Packages are specified as dictionaries with mandatory element ``name`` and
optional elements ``dependencies`` and ``repos``.

The ``files`` element refers to files in the repository (``src`` is the file path relative
to the repository root) that are copied into the created Docker image (``dst``).

Docker images will be built once at the time when the operators are registered.
"""

from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from pymongo.database import Database
from typing import Any, Dict, List, Optional, Tuple

import git
import tempfile
import yaml

from drama.core.docker.build import docker_build
from drama.manager import BaseManager


# -- Operator specifications --------------------------------------------------

@dataclass
class InputFile:
    """
    Input file specification.
    """
    # The source for an input file is specified as a label that references a
    # resource that is defined in the inputs of a TaskRequest.
    src: str
    # Relative path specifying the file location inside the Docker container.
    dst: str


@dataclass
class OutputFile:
    """
    Output file specification.
    """
    # Relative path specifying the file location inside the Docker container.
    src: str
    # Some form of serialization for the file DataType.
    datatype: Dict
    # Label for referencing the output file in the input specification of other
    # workflow tasks.
    label: str


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
    def __init__(self, doc: Dict, version: str):
        """
        Initialize the different attributes and properties fro a given dictionary
        serialization of the operator specification.

        Parameters
        ----------
        doc: dict
            Dictionary serialization of the specification for the operator.
        version: string
            Version identifier for the registered operator.
        """
        self._doc = doc
        self.version = version
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
                InputFile(src=obj["src"], dst=obj["dst"]) for obj in in_files
            ],
            outputs=[
                OutputFile(
                    src=obj["src"],
                    datatype=obj["datatype"],
                    label=obj.get("label", obj["src"])
                ) for obj in out_files
            ]
        )
        self.parameters = [
            OpParameter(
                name=obj["name"],
                type=obj["type"],
                default=obj.get("default")
            ) for obj in doc.get("parameters", [])
        ]

    def to_dict(self) -> Dict:
        """
        Get dictionary serialization for the operator specification.

        Returns
        -------
        dict
        """
        return self._doc


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
    def list_ops(self) -> List[Tuple[str, DockerOp]]:
        """
        Get listing of all registered operators.

        Returns tuples of operator identifier and operator specification.

        Returns
        -------
        list of tuple (string, DockerOp)
        """
        pass

    @abstractmethod
    def put_op(self, identifier: str, version: str, spec: Dict, replace: Optional[bool] = False):
        """
        Add specification for a new operator to the registry.

        Registers the operator under the given identifier. If an operator
        with that identifier already exists, it will be replaced if the
        ``replace`` flag is *True* or a ValueError is raised if the flag is
        *False*.

        The specification ``spec`` contains (i) the lists of input and output files
        for the operator, (ii) the additional (user-provided) parameter values, and
        (iii) the command line statements for running the operator in a Docker container.

        Parameters
        ----------
        identifier: string
            Unique operator identifier.
        version: string
            Operator version identifier.
        spec: dict
            Specification of the operator inputs, outputs, parameters, and
            command line statements for execution within a Docker container.
        replace: bool, default=False
            Replace existing operators if True.
        """
        pass

    def register(self, source: str, specfile: Optional[str] = None, replace: Optional[bool] = False) -> List[str]:
        """
        Register external operators implemented by the files in a given source
        folder, and a specification file.

        The ``source`` either references a folder on the local file system or
        contains the URL of a Git repository. If a repository URL is given,
        the repository will be cloned and the specified operators added to the
        registry.

        The ``specfile`` argument allows to specify the document (relative to
        the source) that contains the operator specification. If not given, the
        default specification file ``drama.yaml`` is assumed.

        If the specification contains the definition of Docker images these
        images will be build locally.

        Returns the identifier of the registered operators.

        Parameters
        ----------
        source: str
            Reference to a folder on the local file system or a Git repository.
        specfile: string, default=None
            Relative path to the specification file in the source folder.
        replace: bool, default=False
            Replace existing operators if True.

        Returns
        -------
        list of string
        """
        registered_ops = list()
        with clone(source) as sourcedir:
            specfilepath = Path(sourcedir, specfile if specfile else "drama.yaml")
            with specfilepath.open("rt") as f:
                doc = yaml.load(f, Loader=yaml.FullLoader)
            # Build any Docker images that are specified in the document.
            for obj in doc.get("dockerImages", []):
                docker_build(sourcedir=sourcedir, spec=obj)
            # Add specifications for external operators.
            version = doc.get("version")
            namespace = doc.get("namespace")
            for obj in doc.get("operators", []):
                op_id = f"{namespace}.{obj['name']}" if namespace else obj['name']
                self.put_op(identifier=op_id, version=version, spec=obj, replace=replace)
                registered_ops.append(op_id)
        return registered_ops


class PersistentRegistry(BaseManager, OpRegistry):
    """
    Implementation for the operator registry that maintains all operators in the
    database (MongoDB) that is used by drama.
    """
    def __init__(self, db: Optional[Database] = None):
        """
        Initialize the database connection.

        Parameters
        ----------
        db: str, Database=None
            Optional database to override the default database.
        """
        super().__init__(db=db)

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
        doc = self.database.catalog.find_one({'opId': identifier})
        if doc is None:
            raise ValueError(f"unknown operator '{identifier}'")
        return DockerOp(doc=doc['spec'], version=doc['version'])

    def list_ops(self) -> List[Tuple[str, DockerOp]]:
        """
        Get listing of all registered operators.

        Returns tuples of operator identifier and operator specification.

        Returns
        -------
        list of tuple (string, DockerOp)
        """
        ops = []
        for doc in self.database.catalog.find():
            ops.append((doc['opId'], DockerOp(doc=doc['spec'], version=doc['version'])))
        return ops

    def put_op(self, identifier: str, version: str, spec: Dict, replace: Optional[bool] = False):
        """
        Add specification for a new operator to the registry.

        Registers the operator under the given identifier. If an operator
        with that identifier already exists, it will be replaced if the
        ``replace`` flag is *True* or a ValueError is raised if the flag is
        *False*.

        The specification ``spec`` contains (i) the lists of input and output files
        for the operator, (ii) the additional (user-provided) parameter values, and
        (iii) the command line statements for running the operator in a Docker container.

        Parameters
        ----------
        identifier: string
            Unique operator identifier.
        version: string
            Operator version identifier.
        spec: dict
            Specification of the operator inputs, outputs, parameters, and
            command line statements for execution within a Docker container.
        replace: bool, default=False
            Replace existing operators if True.
        """
        doc = {'opId': identifier, 'spec': spec, 'version': version}
        # Check if an operator with the given identifier exists. Note that this
        # is not executed inside a transaction so that there can be situations
        # in which different operators may still be inserted under the same
        # identifier.
        query = {'opId': identifier}
        if self.database.catalog.find_one(query) is not None:
            if not replace:
                # Raise an error if a document with the given identifier exists
                # and the replace flag is False.
                raise ValueError(f"operator '{identifier}' exists")
            # Otherwise, replace the existing document.\
            self.database.catalog.update_one(query, {"$set": doc})
        else:
            # Insert a new operator.
            self.database.catalog.insert_one(doc)


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

    def list_ops(self) -> List[DockerOp]:
        """
        Get listing of all registered operators.

        Returns
        -------
        list of DockerOp
        """
        return self._operators.values()

    def put_op(self, identifier: str, version: str, spec: Dict, replace: Optional[bool] = False):
        """
        Add specification for a new operator to the registry.

        Registers the operator under the given identifier. If an operator
        with that identifier already exists, it will be replaced if the
        ``replace`` flag is *True* or a ValueError is raised if the flag is
        *False*.

        The specification ``spec`` contains (i) the lists of input and output files
        for the operator, (ii) the additional (user-provided) parameter values, and
        (iii) the command line statements for running the operator in a Docker container.

        Parameters
        ----------
        identifier: string
            Unique operator identifier.
        version: string
            Operator version identifier.
        spec: dict
            Specification of the operator inputs, outputs, parameters, and
            command line statements for execution within a Docker container.
        replace: bool, default=False
            Replace existing operators if True.
        """
        if identifier in self._operators and not replace:
            raise ValueError(f"operator '{identifier}' exists")
        self._operators[identifier] = DockerOp(doc=spec, version=version)


# -- Container registry -------------------------------------------------------

class ContainerRegistry(BaseManager):
    """
    Registry that maintains a record for running Docker containers. Stores the
    workflow identifier and the container identifier. If a workflow is canceled
    the registry provides information about running Docker containers for the
    workflow that need to be stopped.
    """
    def __init__(self, db: Optional[Database] = None):
        """
        Initialize the database connection.

        Parameters
        ----------
        db: str, Database=None
            Optional database to override the default database.
        """
        super().__init__(db=db)

    def find_all(self, workflow: str) -> List[str]:
        """
        Get list of all containers that are associated with a given workflow.

        Parameters
        ----------
        workflow: string
            Unique workflow identifier.

        Returns
        -------
        list of string
        """
        query = {"workflow": workflow}
        return [doc['container'] for doc in self.database.containers.find(query)]

    def insert(self, workflow: str, container: str):
        """
        Insert a new record into the registry.

        Parameters
        ----------
        workflow: string
            Unique workflow identifier.
        container: string
            Unique container identifier
        """
        self.database.containers.insert_one({
            "workflow": workflow,
            "container": container
        })

    def remove(self, workflow: str, container: str):
        """
        Remove a record from the registry.

        Parameters
        ----------
        workflow: string
            Unique workflow identifier.
        container: string
            Unique container identifier
        """
        self.database.containers.delete_one({
            "workflow": workflow,
            "container": container
        })


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
        print(f'cloning {source} into {sourcedir}')
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
