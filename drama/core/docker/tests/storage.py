from pathlib import Path

import shutil

from drama.core.docker.base import unique_id
from drama.storage.backend.local import LocalResource
from drama.storage.base import Resource


class FileSystemStore:
    """
    Simulates the persistent storage of files for workflow runs. Creates two
    sub-folders under a given base directory:

    - *store*: Folder for storing files that are stored in the global data
      catalog.
    - *run*: Folder for workflow run files that are kept for a successful
      workflow run.
    """
    def __init__(self, basedir: str):
        self.basedir = Path(basedir)
        self.basedir.mkdir(exist_ok=True, parents=True)
        # Create sub-folders for the global store and the workflow run.
        self.glob = FolderStore(basedir=Path(self.basedir, 'store'))
        self.run = FolderStore(basedir=Path(self.basedir, 'run'))
        # Keep track of temporary directories for cleanup.
        self._tmpdirs = list()

    def cleanup(self):
        """
        Erase all temporary folders that were created.
        """
        for filepath in self._tmpdirs:
            shutil.rmtree(filepath)

    def tmpdir(self) -> Path:
        """
        Create a new temporary folder for a Docker run.
        """
        filepath = Path(self.basedir, unique_id())
        filepath.mkdir()
        self._tmpdirs.append(filepath)
        return filepath


class FolderStore:
    """
    Implemenatation of get and put methods for file storage.
    """
    def __init__(self, basedir: Path):
        self.basedir = basedir
        self.basedir.mkdir(exist_ok=True)

    def get_file(self, path: str) -> Resource:
        filepath = Path(self.basedir, path)
        return LocalResource(resource=str(filepath))

    def put_file(self, src: Path, dst: str):
        filepath = Path(self.basedir, dst)
        filepath.absolute().parent.mkdir(exist_ok=True, parents=True)
        shutil.copy2(src=src, dst=filepath)
        return LocalResource(resource=str(filepath))
