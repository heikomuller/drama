"""
This module contains a collection of base type definitions and helper functions
for creating and running workflow steps using Docker containers.
"""

from pathlib import Path
from typing import List, Union

import traceback
import uuid


"""Type alias for path expressions that may either be represented as strings
or pathlib.Path objects.
"""
Pathname = Union[str, Path]


# -- Helper Functions ---------------------------------------------------------

def stacktrace(ex) -> List[str]:
    """
    Get list of strings representing the stack trace for a given exception.

    Parameters
    ----------
    ex: Exception
        Exception that was raised.

    Returns
    -------
    list of string
    """
    try:
        st = traceback.format_exception(type(ex), ex, ex.__traceback__)
    except (AttributeError, TypeError):
        st = [str(ex)]
    return [line.strip() for line in st]


def unique_id() -> str:
    """Create a new unique identifier.

    The result is a 32 character string generated from a random UUID.

    Returns
    -------
    string
    """
    return str(uuid.uuid4()).replace('-', '')
