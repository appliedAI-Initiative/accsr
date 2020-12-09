import logging
import os
import re
import tarfile
import urllib.request
from contextlib import contextmanager
from io import BufferedReader
from os import PathLike
from typing import Dict, Optional, Union

from tqdm import tqdm

log = logging.getLogger(__name__)


def download_file(
    url: str,
    output_file: Union[str, PathLike],
    show_progress=False,
    overwrite_existing=False,
    headers: Optional[Dict[str, str]] = None,
):
    """
    Download a file via HTTP[S] to a specified directory

    :param url: URL of the file to be downloaded
    :param output_file: Destination path for the downloaded file
    :param show_progress: show a progress bar using :mod:`tqdm`
    :param overwrite_existing: whether to overwrite existing files
    :param headers: Optional headers to add to request, e.g. {"Authorization": "Bearer <access_token>" }
    """
    if os.path.exists(output_file):
        if overwrite_existing:
            log.info(f"Overwriting existing file {output_file}")
        else:
            raise FileExistsError(f"{output_file} exists, skipping download")

    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    if headers:
        headers_list = [(k, v) for k, v in headers.items()]
        opener = urllib.request.build_opener()
        opener.addheaders = headers_list
        urllib.request.install_opener(opener)
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
    path: Union[str, PathLike], file_regex: Union[str, re.Pattern] = ".*"
) -> BufferedReader:
    """
    Opens an archived file in memory without extracting it on disc. Use as context manager:
    >>> with open_file_in_tar(...) as fh: pass

    :param path: Local file path to the tar archive.
    :param file_regex: A regular expression which will be matched against the files in the archive.
      The matching file will be returned.

    :raises `ValueError`: when the `file_regex` matches multiple or no file in the archive.
    """
    if isinstance(file_regex, str):
        file_regex = re.compile(file_regex)

    with tarfile.open(path) as tar:
        file_names = tar.getnames()
        matches = list(filter(file_regex.match, file_names))
        if len(matches) != 1:
            raise ValueError(
                f"Regular expression {file_regex.pattern} matched against zero or multiple files {matches}"
            )
        file_name = matches[0]
        log.debug(f"Yielding {file_name} from {path}")
        with tar.extractfile(file_name) as file:
            yield file
