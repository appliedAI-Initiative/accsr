import os

import pytest

from accsr.loading import open_file_in_tar

SAMPLE_TAR = "sample.tar"
SAMPLE_TXT = "sample.txt"
SAMPLE_TIFF = "sample.tiff"


@pytest.fixture(autouse=True)
def sample_tar(test_resources):
    return os.path.join(test_resources, SAMPLE_TAR)


def test_open_file_in_tar_file_fileIndexingWorks(sample_tar):
    with open_file_in_tar(sample_tar, file_index=1) as file:
        indexed_file_content = file.read()
    with open_file_in_tar(sample_tar, file_index=2) as file:
        wrong_indexed_file_content = file.read()
    with open_file_in_tar(sample_tar, file_name=SAMPLE_TXT) as file:
        named_file_content = file.read()
    assert indexed_file_content == named_file_content
    assert wrong_indexed_file_content != indexed_file_content


def test_open_file_in_tar_inputValidation(sample_tar):
    with pytest.raises(FileNotFoundError):
        with open_file_in_tar(sample_tar, file_name="nonexisting_file"):
            pass
    with pytest.raises(IndexError):
        with open_file_in_tar(sample_tar, file_index=3):
            pass
    with pytest.raises(ValueError):
        with open_file_in_tar(sample_tar):
            pass
    with pytest.raises(ValueError):
        with open_file_in_tar(sample_tar, file_name=SAMPLE_TIFF, file_index=2):
            pass


def test_open_file_in_tar_file_closedAfterLoading(sample_tar):
    with open_file_in_tar(sample_tar, file_index=1) as file:
        pass
    assert file.closed


def test_open_file_in_tar_file_openInWithUsage(sample_tar):
    with open_file_in_tar(sample_tar, file_index=1) as file:
        assert not file.closed


def test_open_file_in_tar_file_trueContentRetrieved(sample_tar, test_resources):
    with open_file_in_tar(sample_tar, file_name=SAMPLE_TXT) as file:
        archived_content = file.read()
    with open(os.path.join(test_resources, SAMPLE_TXT), "rb") as file:
        true_content = file.read()
    assert archived_content == true_content
