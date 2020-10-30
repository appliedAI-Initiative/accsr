import hashlib
from pathlib import Path


def md5sum(path: str) -> str:
    """
    Calculate the MD5 hash of a file as a hex digest
    :param path:
    :return:
    """
    p = Path(path)
    assert p.is_file()

    return hashlib.md5(p.read_bytes()).hexdigest()
