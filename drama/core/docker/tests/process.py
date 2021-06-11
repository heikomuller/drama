from drama.core.docker.registry import VolatileRegistry
from drama.core.docker.tests.storage import FileSystemStore
from drama.storage.base import Resource


class PCS:
    """
    Process class for test purposes. Implements a subset and variation of the
    :class:``drama.process.Process`` class.
    """
    def __init__(self, basedir: str, sourcedir: str):
        self.storage = FileSystemStore(basedir=basedir)
        self.catalog = VolatileRegistry(source=sourcedir)
        self._context = list()

    def to_downstream(self, resource: Resource, tag: str):
        """
        Add resource with a given tag to the process context. This will make the
        resource available via the tag name for downstream processes.
        """
        self._context.append((resource, tag))

    def upstream_one(self, query: str) -> Resource:
        """
        Query the process context to get a resource based on its tag name.
        """
        for resource, tag in self._context:
            if tag == query:
                return resource
        raise ValueError(f"unknown resource {query}")
