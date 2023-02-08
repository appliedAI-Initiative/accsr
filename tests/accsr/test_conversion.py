import numpy as np
from pytest import mark

from accsr.conversions import to_json_compatible_dict, to_json_compatible_value


@mark.parametrize(
    ["value_to_convert", "expected_value"],
    [
        (np.array([1, 2, 3]), [1, 2, 3]),
        (np.array([[1, 2], [3, 4]]), [[1, 2], [3, 4]]),
        (np.array([1.0, 2.0, 3.0]), [1.0, 2.0, 3.0]),
        (np.int64(10), 10),
        (np.float32(10.5), 10.5),
        (np.float64(10.5), 10.5),
        (np.int32(1), 1),
        (np.int64(2), 2),
        (np.uint32(3), 3),
        (np.inf, "inf"),
        (np.nan, "nan"),
    ],
)
def test_to_json_compatible_value(value_to_convert, expected_value):
    assert to_json_compatible_value(value_to_convert) == expected_value


@mark.parametrize(
    ["dict_to_convert", "expected_dict"],
    [
        ({"a": np.int32(1), "b": np.array([1, 2])}, {"a": 1, "b": [1, 2]}),
    ],
)
def test_to_json_compatible_dict(dict_to_convert, expected_dict):
    assert to_json_compatible_dict(dict_to_convert) == expected_dict
