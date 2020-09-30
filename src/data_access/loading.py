import logging
import os
import tarfile
import urllib.request
from contextlib import contextmanager
from io import BufferedReader
from os import PathLike
from typing import Union

from tqdm import tqdm

log = logging.getLogger(__name__)


def download_file(
    url: str,
    output_file: Union[str, PathLike],
    show_progress=False,
    overwrite_existing=False,
):
    """
    Download a file via HTTP[S] to a specified directory

    :param url: URL of the file to be downloaded
    :param output_file: Destination path for the downloaded file
    :param show_progress: show a progress bar using :mod:`tqdm`
    :param overwrite_existing: whether to overwrite existing files
    :return:
    """
    if os.path.exists(output_file):
        if overwrite_existing:
            log.info(f"Overwriting existing file {output_file}")
        else:
            raise FileExistsError(f"{output_file} exists, skipping download")

    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    if show_progress:
        with tqdm(desc=output_file, unit="B", unit_scale=True) as progress:

            def update_progress(_, read_size, total_size):
                progress.total = total_size
                progress.update(read_size)

            urllib.request.urlretrieve(url, output_file, reporthook=update_progress)
    else:
        urllib.request.urlretrieve(url, output_file)


@contextmanager
def open_file_in_tar(
    path: str, file_name: str = None, file_index: int = None
) -> BufferedReader:
    """
    Opens an archived file in memory without extracting it on disc. Use as context manager
    >>> with open_file_in_tar(...) as fh: pass

    :param path:
    :param file_name:
    :param file_index: 1-based index of the file to retrieve
    :return:
    """
    if file_name is not None and file_index is not None:
        raise ValueError("Either file_name or file_index should be passed; not both")
    if file_name is None and file_index is None:
        raise ValueError("One of file_name or file_index has to be passed")

    with tarfile.open(path) as tar:
        archived_files = tar.getnames()
        if file_index is not None:
            if file_index < 1:
                raise IndexError(
                    f"Invalid index {file_index}. NOTE: the parameter file_index is 1 based"
                )
            file_name = archived_files[file_index - 1]  # tar uses 1-based indices
        # tar.extractfile returns None for non-existing files, so we have to raise the Exception ourselves
        elif file_name not in archived_files:
            raise FileNotFoundError(f"No such file in {path}: {file_name}")
        log.debug(f"Yielding {file_name} from {path}")

        with tar.extractfile(file_name) as file:
            yield file
