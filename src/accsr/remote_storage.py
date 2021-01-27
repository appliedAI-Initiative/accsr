import logging.handlers
import os
import re
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import List, Optional

import libcloud
from libcloud.storage.base import Container, Object, StorageDriver
from libcloud.storage.types import ObjectDoesNotExistError

from accsr.files import md5sum

log = logging.getLogger(__name__)


class Provider(str, Enum):
    GOOGLE_STORAGE = "google_storage"
    S3 = "s3"


@dataclass
class RemoteStorageConfig:
    provider: str
    key: str
    bucket: str
    secret: str = field(repr=False)
    region: str = None
    host: str = None
    port: int = None
    base_path: str = ""


class RemoteStorage:
    """
    Wrapper around lib-cloud for accessing remote storage services.

    :param conf:
    """

    def __init__(self, conf: RemoteStorageConfig):
        self._bucket: Optional[Container] = None
        self.conf = conf
        self.provider = conf.provider
        self.remote_base_path = conf.base_path
        possible_driver_kwargs = {
            "key": self.conf.key,
            "secret": self.conf.secret,
            "region": self.conf.region,
            "host": self.conf.host,
            "port": self.conf.port,
        }
        self.driver_kwargs = {
            k: v for k, v in possible_driver_kwargs.items() if v is not None
        }

    @property
    def bucket(self):
        return self._maybe_instantiate_bucket()

    def _maybe_instantiate_bucket(self):
        if self._bucket is None:
            log.info(f"Establishing connection to bucket {self.conf.bucket}")
            storage_driver_factory = libcloud.get_driver(
                libcloud.DriverType.STORAGE, self.provider
            )
            driver: StorageDriver = storage_driver_factory(**self.driver_kwargs)
            self._bucket: Container = driver.get_container(self.conf.bucket)
        return self._bucket

    def pull_file(
        self, remote_path: str, local_base_dir=None, overwrite_existing=False
    ) -> Optional[Object]:
        """
        Pull a file from remote storage. If a file with the same name already exists locally,
        will not download anything unless overwrite_existing is True

        :param remote_path: remote path on storage bucket relative to the configured remote base path.
            e.g. 'data/ground_truth/some_file.json'
        :param local_base_dir: Local base directory for constructing local path
            e.g 'my/data/dir' yields the path
            'my/data/dir/data/ground_truth/some_file.json' for the above remote_path example
        :param overwrite_existing: Whether to overwrite_existing existing local files
        :return: if a file was downloaded, returns a :class:`Object` instance referring to it
        """
        if local_base_dir is None:
            local_base_dir = ""
        download_path = os.path.join(local_base_dir, remote_path)
        if os.path.exists(download_path):
            if not overwrite_existing:
                log.info(
                    f"File {download_path} already exists locally, skipping download"
                )
                return

        remote_path = "/".join([self.remote_base_path, remote_path])
        log.debug(f"Fetching {remote_path} from {self.bucket.name}")
        remote_object = self.bucket.get_object(remote_path)
        os.makedirs(os.path.dirname(download_path), exist_ok=True)
        remote_object.download(download_path, overwrite_existing=overwrite_existing)
        log.debug(f"Downloaded {remote_path} to {os.path.abspath(download_path)}")
        return remote_object

    def pull(
        self,
        path: str,
        local_base_dir=None,
        overwrite_existing=False,
        file_pattern: str = None,
    ):
        """
        Pull either a file or a directory under the given path relative to local_base_dir. Files with the same name
        as locally already existing ones will not be downloaded anything unless overwrite_existing is True

        :param path: remote path on storage bucket relative to the configured remote base path.
            e.g. 'data/ground_truth/some_file.json'
        :param local_base_dir: Local base directory for constructing local path
            e.g 'local_base_dir' yields a path
            'local_base_dir/data/ground_truth/some_file.json' in the above example
        :param overwrite_existing: Overwrite file if exists locally
        :param file_pattern: Use a regular expression to match the filename.
        :return: list of :class:`Object` instances referring to all downloaded files
        """
        remote_path_prefix_len = len(self.remote_base_path) + 1
        remote_path = "/".join([self.remote_base_path, path])
        remote_objects = list(self.bucket.list_objects(remote_path))
        if len(remote_objects) == 0:
            log.warning(
                f"No such remote file or directory: {remote_path}. Not pulling anything"
            )
            return []
        downloaded_objects = []

        for remote_obj in remote_objects:
            # Due to a possible bug in libcloud or storage providers, directories may be listed here.
            # We filter them out by checking for size
            if remote_obj.size == 0:
                log.info(f"Skipping download of {remote_obj.name} with size zero.")
                continue

            # removing the remote prefix from the full path
            remote_obj_path = remote_obj.name[remote_path_prefix_len:]
            if file_pattern is not None:
                rel_file_path = os.path.relpath(remote_obj_path, path)
                if not re.match(file_pattern, rel_file_path):
                    log.info(f"Skipping {rel_file_path} due to regex {file_pattern}")
                    continue

            downloaded_object = self.pull_file(
                remote_obj_path,
                local_base_dir=local_base_dir,
                overwrite_existing=overwrite_existing,
            )
            if downloaded_object is not None:
                downloaded_objects.append(downloaded_object)

        return downloaded_objects

    @staticmethod
    def _get_push_local_path(path: str, local_path_prefix: Optional[str] = None) -> str:
        """
        Get the full local path of a file for pushing, including an optional path prefix.

        Note that ``path`` may not be absolute if ``local_path_prefix`` is specified.

        **Usage Examples:**

        >>> RemoteStorage._get_push_local_path('/foo/bar/baz.txt')
        '/foo/bar/baz.txt'
        >>> RemoteStorage._get_push_local_path('foo/bar/baz.txt')
        'foo/bar/baz.txt'
        >>> RemoteStorage._get_push_local_path('bar/baz.txt', local_path_prefix='/foo')
        '/foo/bar/baz.txt'
        >>> RemoteStorage._get_push_local_path('/bar/baz.txt', local_path_prefix='/foo')
        Traceback (most recent call last):
        ...
        ValueError: /bar/baz.txt is an absolute path and local_path_prefix was specified

        :param path:
        :param local_path_prefix:
        :return:
        """
        # Parameter validation
        if local_path_prefix and Path(path).is_absolute():
            raise ValueError(
                f"{path} is an absolute path and local_path_prefix was specified"
            )

        if Path(path).is_absolute():
            return path
        else:
            return os.path.join(local_path_prefix or "", path)

    def _get_push_remote_path(self, local_path: str) -> str:
        """
        Get the full path within a remote storage bucket for pushing.

        :param local_path:
        :return:
        """
        return "/".join([self.remote_base_path, local_path]).replace("//", "/")

    def push_directory(
        self,
        path: str,
        local_path_prefix: Optional[str] = None,
        overwrite_existing=True,
        file_pattern: str = None,
    ) -> List[Object]:
        """
        Upload a directory from the given local path into the remote storage. The remote path to
        which the directory is uploaded will be constructed from the remote_base_path and the provided path. The
        local_path_prefix serves for finding the directory on the local system.

        Examples:
           1) path=foo/bar, local_path_prefix=None -->
                ./foo/bar uploaded to remote_base_path/foo/bar
           2) path=/home/foo/bar, local_path_prefix=None -->
                /home/foo/bar uploaded to remote_base_path/home/foo/bar
           3) path=bar, local_path_prefix=/home/foo -->
                /home/foo/bar uploaded to remote_base_path/bar

        Note that ``path`` may not be absolute if ``local_path_prefix`` is specified.

        :param path: Path to the local directory to be uploaded, may be absolute or relative
        :param local_path_prefix: Optional prefix for the local path
        :param overwrite_existing: If a remote object already exists, overwrite it?
        :param file_pattern: Use a regular expression to match the filename.
        :return: A list of :class:`Object` instances for all created objects
        """
        log.debug(f"push_object({path=}, {local_path_prefix=}, {overwrite_existing=}")
        objects = []

        local_path = self._get_push_local_path(path, local_path_prefix)
        if not os.path.isdir(local_path):
            raise FileNotFoundError(
                f"Local path {local_path} does not refer to a directory"
            )

        for root, _, files in os.walk(local_path):
            log.debug(f"Root directory: {root}")
            log.debug(f"Files: {files}")

            root_path = Path(root)
            for file in files:
                if file_pattern is not None:
                    full_path = os.path.join(root, file)
                    rel_file_path = os.path.relpath(local_path_prefix, full_path)
                    if not re.match(file_pattern, rel_file_path):
                        log.info(
                            f"Skipping {rel_file_path} due to regex {file_pattern}"
                        )
                        continue

                log.debug(f"Upload: {file=}, {root_path=}")
                obj = self.push_file(
                    file, root_path, overwrite_existing=overwrite_existing
                )
                objects.append(obj)
        return objects

    def push_file(
        self,
        path: str,
        local_path_prefix: Optional[str] = None,
        overwrite_existing=True,
    ) -> Object:
        """
        Upload a local file into the remote storage. The remote path to
        which the file is uploaded will be constructed from the remote_base_path and the provided path. The
        local_path_prefix serves for finding the file on the local system.

        Examples:
           1) path=foo/bar.json, local_path_prefix=None -->
                ./foo/bar.json uploaded to remote_base_path/foo/bar.json
           2) path=/home/foo/bar.json, local_path_prefix=None -->
                /home/foo/bar.json uploaded to remote_base_path/home/foo/bar.json
           3) path=bar.json, local_path_prefix=/home/foo -->
                /home/foo/bar.json uploaded to remote_base_path/bar.json

        Note that ``path`` may not be absolute if ``local_path_prefix`` is specified.

        :param path: Path to the local file to be uploaded, must not be absolute if ``local_path_prefix`` is specified
        :param local_path_prefix: Prefix to be concatenated with ``path``
        :param overwrite_existing: If the remote object already exists, overwrite it?
        :return: A :class:`Object` instance referring to the created object
        """
        log.debug(
            f"push_file({path=}, {local_path_prefix=}, {self.remote_base_path=}, {overwrite_existing=}"
        )

        local_path = self._get_push_local_path(path, local_path_prefix)
        if not os.path.isfile(local_path):
            raise FileNotFoundError(f"Local path {local_path} does not refer to a file")
        remote_path = self._get_push_remote_path(local_path)

        remote_obj = self.bucket.list_objects(remote_path)
        if remote_obj and not overwrite_existing:
            raise RuntimeError(
                f"Remote object {remote_path} already exists and overwrite_existing=False"
            )

        # Skip upload if MD5 hashes match
        if remote_obj:
            remote_obj = remote_obj[0]
            if md5sum(local_path) == remote_obj.hash:
                log.info(f"Files are identical, not uploading again")
                return remote_obj

        log.debug(f"Uploading: {local_path} --> {remote_path}")
        remote_obj = self.bucket.upload_object(
            local_path, remote_path, verify_hash=False
        )
        return remote_obj

    def push(
        self,
        path: str,
        local_path_prefix: Optional[str] = None,
        overwrite_existing=True,
        file_pattern: str = None,
    ) -> List[Object]:
        """
        Upload a local file or directory into the remote storage. The remote path for uploading
        will be constructed from the remote_base_path and the provided path. The
        local_path_prefix serves for finding the directory on the local system.

        Examples:
           1) path=foo/bar, local_path_prefix=None -->
                ./foo/bar uploaded to remote_base_path/foo/bar
           2) path=/home/foo/bar, local_path_prefix=None -->
                /home/foo/bar uploaded to remote_base_path/home/foo/bar
           3) path=bar, local_path_prefix=/home/foo -->
                /home/foo/bar uploaded to remote_base_path/bar

        Note that ``path`` may not be absolute if ``local_path_prefix`` is specified.

        Remote objects will not be overwritten if their MD5sum matches the local file.

        :param path: Path to the local object (file or directory) to be uploaded, may be absolute or relative
        :param local_path_prefix: Prefix to be concatenated with ``path``
        :param overwrite_existing: If a remote object already exists, overwrite it?
        :param file_pattern: Use a regular expression to match the filename.
        :return:
        """
        local_path = self._get_push_local_path(path, local_path_prefix)
        if os.path.isfile(local_path):
            return [self.push_file(path, local_path_prefix, overwrite_existing)]
        elif os.path.isdir(local_path):
            return self.push_directory(
                path, local_path_prefix, overwrite_existing, file_pattern
            )
        else:
            raise FileNotFoundError(
                f"Local path {local_path} does not refer to a file or directory"
            )
