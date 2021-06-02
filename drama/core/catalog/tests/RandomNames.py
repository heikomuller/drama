from io import StringIO
from pathlib import Path
from random import Random
from typing import Optional

import requests

from drama.core.annotation import TaskMeta, annotation
from drama.core.model import TempFile
from drama.models.task import TaskResult
from drama.process import Process


@annotation(
    TaskMeta(
        name="RandomNames",
        desc="Create random sample of names.",
        outputs=TempFile,
        params=[("sample_size", int), ("random_state", int)],
    )
)
def execute(pcs: Process, sample_size: int, random_state: Optional[int] = None) -> TaskResult:
    """
    Create a random sample if size ``sample_size`` from names in the list of
    popular female names at:
    https://www.gutenberg.org/files/3201/files/NAMES-F.TXT
    """
    # Load list of names.
    url = 'https://www.gutenberg.org/files/3201/files/NAMES-F.TXT'
    r = requests.get(url)
    r.raise_for_status()
    with StringIO(r.text, newline='') as f:
        names = [line.strip() for line in f]
    # Create random sample.
    rand = Random()
    rand.seed(random_state)
    sample = rand.sample(names, k=sample_size)
    # Write name sample to file.
    filepath = Path(pcs.storage.local_dir, 'names.txt')
    with filepath.open('wt') as f:
        for name in sample:
            f.write(f'{name}\n')
    # Send to remote storage
    names_file = pcs.storage.put_file(filepath)
    # Send to downstream
    output_temp_file = TempFile(resource=names_file)
    pcs.to_downstream(output_temp_file)
    # Return task result.
    return TaskResult(files=[names_file])
