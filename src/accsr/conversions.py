from math import isinf, isnan
from typing import Any, Dict, Union

import numpy as np


def to_json_compatible_value(value):
    """
    Cast a numerical value to be compatible with json serialization.

    >>> import numpy
    >>> from accsr.conversions import to_json_compatible_value
    >>> to_json_compatible_value(numpy.array([1, 2, 3]))
    [1, 2, 3]
    >>> to_json_compatible_value(numpy.array([[1], [2], [3]]))
    [[1], [2], [3]]
    >>> to_json_compatible_value(numpy.nan)
    'nan'
    >>> to_json_compatible_value(3.3)
    3.3

    """
    if isinstance(value, np.ndarray):
        value = value.tolist()
    elif isinstance(value, np.integer):
        value = int(value)
    elif isinstance(value, np.floating):
        value = float(value)
    elif not isinstance(value, float) and not isinstance(value, int):
        value = str(value)

    if isinstance(value, float) and (isinf(value) or isnan(value)):
        value = str(value)
    return value


def to_json_compatible_dict(
    d: Dict[Union[str, int], Any]
) -> Dict[Union[str, int], Any]:
    """
    Calls the to_json_compatible_value function for each dict entry.
    Does not support nested dicts.

    >>> from accsr.conversions import to_json_compatible_dict
    >>> import numpy
    >>> to_json_compatible_dict({'a': numpy.int32(1), 'b': numpy.array([1, 2])})
    {'a': 1, 'b': [1, 2]}
    """
    return {k: to_json_compatible_value(v) for k, v in d.items()}
