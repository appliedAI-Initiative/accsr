from typing import Union, Dict, Any

from math import isinf, isnan
import numpy as np


def to_json_compatible_value(value):
    if isinstance(value, np.ndarray):
        value = value.tolist()
    elif isinstance(value, np.int64):
        value = int(value)
    elif isinstance(value, np.float):
        value = float(value)
    elif not isinstance(value, float) and not isinstance(value, int):
        value = str(value)

    if isinstance(value, float) and (isinf(value) or isnan(value)):
        value = str(value)
    return value


def to_json_compatible_dict(d: Dict[Union[str, int], Any]):
    return {k: to_json_compatible_value(v) for k, v in d.items()}
