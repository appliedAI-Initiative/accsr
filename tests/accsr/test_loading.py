import os
from typing import Any, Tuple, Union

import pytest
from pytest import mark

from accsr.loading import open_file_in_tar

SAMPLE_TGZ = "sample.tgz"
SAMPLE_TXT = "sample.txt"
SAMPLE_TXT_2 = "sample_2.txt"


@pytest.fixture(autouse=True)
def sample_tar(test_resources):
    return os.path.join(test_resources, SAMPLE_TGZ)


@mark.parametrize(
    ["file_regex", "expected_content"],
    [
        ("^[a-z]*.txt$", SAMPLE_TXT),
        ("^[a-z]*_2.txt$", SAMPLE_TXT_2),
    ],
)
def test_open_file_in_tar_content_correct(
    file_regex: str,
    expected_content: str,
    test_resources: str,
    sample_tar: str,
):
    with open_file_in_tar(sample_tar, file_regex=file_regex) as buffer_reader:
        actual_content = buffer_reader.read()
    with open(os.path.join(test_resources, expected_content), "rb") as file:
        expected_content = file.read()
    # Windows...
    # expected_content = expected_content.replace(br"\r\n", br"\n")

    assert str(expected_content) == str(actual_content)


@mark.parametrize(
    ["file_regex", "error"],
    [
        ("sample", ValueError),
        ("noresult", ValueError),
    ],
)
def test_open_file_in_tar_input_validation(
    file_regex: str,
    error: Union[Tuple[Any, ...], Any],
    sample_tar: str,
):
    with pytest.raises(error):
        with open_file_in_tar(sample_tar, file_regex=file_regex):
            pass


@mark.parametrize(
    "file_regex",
    ["^[a-z]*.txt$", "^[a-z]*_2.txt$"],
)
def test_open_file_in_tar_closed_after_loading(file_regex: str, sample_tar: str):
    with open_file_in_tar(sample_tar, file_regex=file_regex) as file:
        pass
    assert file.closed


@mark.parametrize(
    "file_regex",
    ["^[a-z]*.txt$", "^[a-z]*_2.txt$"],
)
def test_open_file_in_tar_open_in_with_usage(file_regex: str, sample_tar: str):
    with open_file_in_tar(sample_tar, file_regex=file_regex) as file:
        assert not file.closed


def test_open_file_tar_default_argument_matches_all(sample_tar: str):
    with pytest.raises(ValueError):
        with open_file_in_tar(sample_tar):
            pass
