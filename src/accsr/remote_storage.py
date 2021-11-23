import logging.handlers
import os
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import List, Optional, Pattern, Protocol

import libcloud
from libcloud.storage.base import Container, StorageDriver

from accsr.files import md5sum

log = logging.getLogger(__name__)


class Provider(str, Enum):
    GOOGLE_STORAGE = "google_storage"
    S3 = "s3"


class RemoteObjectProtocol(Protocol):
    name: str
    size: int
    hash: int
    provider: str

    def download(
        self, download_path, overwrite_existing=False
    ) -> Optional["RemoteObjectProtocol"]:
        pass


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
        self._conf = conf
        self._provider = conf.provider
        self._remote_base_path: str = None
        self.set_remote_base_path(conf.base_path)
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
    def conf(self):
        return self._conf

    @property
    def provider(self):
        return self._provider

    @property
    def remote_base_path(self):
        return self._remote_base_path

    def set_remote_base_path(self, path: Optional[str]):
        if path is None:
            path = ""
        else:
            # google storage pulling and listing does not work with paths starting with "/"
            path = path.strip().lstrip("/")
        self._remote_base_path = path.strip()

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

    @staticmethod
    def _get_remote_path(remote_obj: RemoteObjectProtocol):
        """
        Returns the full path to the remote object. The resulting path never starts with "/" as it can cause problems
        with some backends (e.g. google cloud storage).
        """
        return remote_obj.name.lstrip("/")

    def _get_relative_remote_path(self, remote_obj: RemoteObjectProtocol):
        """
        Returns the path to the remote object relative to configured base dir (as expected by pull for a single file)
        """
        result = remote_obj.name
        result = result[len(self.remote_base_path) :]
        result = result.lstrip("/")
        return result

    def _pull_object(
        self,
        remote_object: RemoteObjectProtocol,
        destination_path: str,
        overwrite_existing=False,
    ) -> bool:
        """
        Download the remote object to the destination path. Returns True if file was downloaded, else False
        """

        destination_path = os.path.abspath(destination_path)
        if os.path.isdir(destination_path):
            raise FileExistsError(
                f"Cannot pull file to a path which is an existing directory: {destination_path}"
            )

        if os.path.isfile(destination_path):
            if not overwrite_existing:
                log.debug(
                    f"Not downloading {remote_object.name} since target file already exists:"
                    f" {os.path.abspath(destination_path)}. Set overwrite_existing to True to force the download"
                )
                return False
            if md5sum(destination_path) == remote_object.hash:
                log.debug(
                    f"File {destination_path} is identical to the pulled file, not downloading again"
                )
                return False

        log.debug(f"Fetching {remote_object.name} from {self.bucket.name}")
        os.makedirs(os.path.dirname(destination_path), exist_ok=True)
        remote_object.download(destination_path, overwrite_existing=overwrite_existing)
        return True

    def _full_remote_path(self, remote_path: str):
        """
        :param remote_path: remote_path on storage bucket relative to the configured remote base remote_path.
            e.g. 'data/some_file.json'
        :return: full remote remote_path on storage bucket. With the example above gives
           "remote_base_path/data/some_file.json". Does not start with "/" even if remote_base_path is empty
        """
        # in google cloud paths cannot begin with / for pulling or listing (for pushing they can though...)
        remote_path = "/".join([self.remote_base_path, remote_path])
        return remote_path.lstrip("/")

    @staticmethod
    def _listed_due_to_name_collision(
        full_remote_path: str, remote_object: RemoteObjectProtocol
    ):
        """
        Checks whether a remote object was falsely listed because its name starts with the same
        characters as full_remote_path.

        Example 1: full remote path is pull/this/dir and the remote storage includes paths like pull/this/dir_subfix.
        Example 2: full remote path is delete/this/file and the remote storage includes paths like delete/this/file_2.

        All such paths will be listed in bucket.list_objects(full_remote_path) and we need to exclude them in
        most methods like pull or delete.

        :param full_remote_path: usually the output of self._full_remote_path(remote_path)
        :param remote_object: the object to check
        :return:
        """
        if full_remote_path.endswith("/"):  # no name collisions possible in this case
            return False

        object_remote_path = RemoteStorage._get_remote_path(remote_object)
        is_in_selected_dir = object_remote_path.startswith(full_remote_path + "/")
        is_selected_file = object_remote_path == full_remote_path
        return not (is_in_selected_dir or is_selected_file)

    def pull(
        self,
        remote_path: str,
        local_base_dir="",
        overwrite_existing=False,
        path_regex: Pattern = None,
        convert_to_linux_path=True,
    ) -> List[RemoteObjectProtocol]:
        r"""
        Pull either a file or a directory under the given path relative to local_base_dir. Files with the same name
        as locally already existing ones will not be downloaded anything unless overwrite_existing is True

        :param remote_path: remote path on storage bucket relative to the configured remote base path.
            e.g. 'data/ground_truth/some_file.json'
        :param local_base_dir: Local base directory for constructing local path
            e.g passing 'local_base_dir' will download to the path
            'local_base_dir/data/ground_truth/some_file.json' in the above example
        :param overwrite_existing: Overwrite file if exists locally
        :param path_regex: If not None only files with paths matching the regex will be pulled. This is useful for
            filtering files within a remote directory before pulling them.
        :param convert_to_linux_path: if True, will convert windows path to linux path (as needed by remote storage) and
            thus passing a remote path like 'data\my\path' will be converted to 'data/my/path' before pulling.
            This should only be set to False if you want to pull a remote object with '\' in its file name
            (which is discouraged).
        :return: list of objects referring to all downloaded files
        """
        local_base_dir = os.path.abspath(local_base_dir)
        if convert_to_linux_path:
            remote_path = remote_path.replace("\\", "/")
        full_remote_path = self._full_remote_path(remote_path)
        remote_objects: List[RemoteObjectProtocol] = list(
            self.bucket.list_objects(full_remote_path)
        )
        if len(remote_objects) == 0:
            log.warning(
                f"No such remote file or directory: {full_remote_path}. Not pulling anything"
            )
            return []

        def maybe_get_destination_path(obj: RemoteObjectProtocol):
            # Due to a possible bug in libcloud or storage providers, directories may be listed in remote objects.
            # We filter them out by checking for size
            if obj.size == 0:
                log.info(f"Skipping download of {obj.name} with size zero.")
                return

            if self._listed_due_to_name_collision(full_remote_path, obj):
                log.debug(
                    f"Skipping download of {obj.name}. "
                    f"It was listed due to name collision and should not be pulled"
                )
                return

            relative_obj_path = self._get_relative_remote_path(obj)
            if path_regex is not None:
                if not path_regex.match(relative_obj_path):
                    log.info(f"Skipping {relative_obj_path} due to regex {path_regex}")
                return
            return os.path.join(local_base_dir, relative_obj_path)

        downloaded_objects = []
        for remote_obj in remote_objects:
            destination_path = maybe_get_destination_path(remote_obj)
            if destination_path is None:
                continue

            was_downloaded = self._pull_object(
                remote_obj,
                destination_path,
                overwrite_existing=overwrite_existing,
            )
            if was_downloaded:
                downloaded_objects.append(remote_obj)

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
        return "/".join([self.remote_base_path, local_path]).replace(os.sep, "/")

    def push_directory(
        self,
        path: str,
        local_path_prefix: Optional[str] = None,
        overwrite_existing=True,
        path_regex: Pattern = None,
    ) -> List[RemoteObjectProtocol]:
        """
        Upload a directory from the given local path into the remote storage. Does not upload files for which the md5sum
        matches existing remote files.
        The remote path to which the directory is uploaded will be constructed from the remote_base_path and the
        provided path. The local_path_prefix serves for finding the directory on the local system.

        Note: This method does not delete any remote objects within the directory where paths did not match the local
        paths, even if overwrite_existing is true

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
        :param overwrite_existing: Whether to overwrite existing remote objects (if they have the same path but differing
            md5sums).
        :param path_regex: If not None only files with paths matching the regex will be pushed.
        :return: A list of :class:`Object` instances for all remote objects that were created or matched existing files
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
            rel_root_path = os.path.relpath(local_path, root)

            root_path = Path(root)
            for file in files:
                if path_regex is not None:
                    remote_obj_path = os.path.join(rel_root_path, file)
                    if not path_regex.match(remote_obj_path):
                        log.info(
                            f"Skipping {remote_obj_path} due to regex {path_regex}"
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
    ) -> Optional[RemoteObjectProtocol]:
        """
        Upload a local file into the remote storage. If the md5sum of the file matches an existing remote file,
        nothing will be uploaded.
        The remote path to which the file is uploaded will be constructed from the remote_base_path and the provided
        path. The local_path_prefix serves for finding the file on the local system.

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
        :return: A :class:`Object` instance referring to the remote object
        """
        log.debug(
            f"push_file({path=}, {local_path_prefix=}, {self.remote_base_path=}, {overwrite_existing=}"
        )

        local_path = self._get_push_local_path(path, local_path_prefix)
        if not os.path.isfile(local_path):
            raise FileNotFoundError(f"Local path {local_path} does not refer to a file")
        remote_path = self._get_push_remote_path(local_path)

        remote_obj = [
            obj
            for obj in self.bucket.list_objects(remote_path)
            if not self._listed_due_to_name_collision(remote_path, obj)
        ]
        if len(remote_obj) > 1:
            raise RuntimeError(
                f"Remote path {remote_path} exists and is a directory, will not overwrite it."
                f"Consider calling push_directory or push instead."
            )

        if remote_obj and not overwrite_existing:
            remote_obj = remote_obj[0]
            # Skip upload if MD5 hashes match
            if md5sum(local_path) == remote_obj.hash:
                log.info(f"Files are identical, skipping upload")
                return remote_obj
            elif not overwrite_existing:
                raise RuntimeError(
                    f"Remote object {remote_path} already exists,\n is not identical to the local file {local_path}\n "
                    f"and overwrite_existing=False"
                )

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
        path_regex: Pattern = None,
    ) -> List[RemoteObjectProtocol]:
        """
        Upload a local file or directory into the remote storage.
        Does not upload files for which the md5sum matches existing remote files.
        The remote path for uploading will be constructed from the remote_base_path and the provided path.
        The local_path_prefix serves for finding the directory on the local system.

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
        :param path_regex: If not None only files with paths matching the regex will be pushed
        :return: A list of :class:`Object` instances for all remote objects that were created or matched existing files
        """
        local_path = self._get_push_local_path(path, local_path_prefix)
        if os.path.isfile(local_path):

            if path_regex is not None and not path_regex.match(path):
                log.warning(
                    f"{path} does not match regular expression '{path_regex}'. Nothing is pushed."
                )
                return []

            return [self.push_file(path, local_path_prefix, overwrite_existing)]

        elif os.path.isdir(local_path):
            return self.push_directory(
                path, local_path_prefix, overwrite_existing, path_regex=path_regex
            )
        else:
            raise FileNotFoundError(
                f"Local path {local_path} does not refer to a file or directory"
            )

    def delete(
        self,
        remote_path: str,
        path_regex: Pattern = None,
    ) -> List[RemoteObjectProtocol]:
        """
        Deletes a file or a directory under the given path relative to local_base_dir. Use with caution.

        :param remote_path: remote path on storage bucket relative to the configured remote base path.
        :param path_regex: If not None only files with paths matching the regex will be deleted.
        :return: list of remote objects referring to all deleted files
        """
        full_remote_path = self._full_remote_path(remote_path)

        remote_objects = self.bucket.list_objects(full_remote_path)
        if len(remote_objects) == 0:
            log.warning(
                f"No such remote file or directory: {full_remote_path}. Not deleting anything"
            )
            return []
        deleted_objects = []
        for remote_obj in remote_objects:
            remote_obj: RemoteObjectProtocol

            if self._listed_due_to_name_collision(full_remote_path, remote_obj):
                log.debug(
                    f"Skipping deletion of {remote_obj.name} as it was listed due to name collision"
                )
                continue

            relative_obj_path = self._get_relative_remote_path(remote_obj)
            if path_regex is not None and not path_regex.match(relative_obj_path):
                log.info(f"Skipping {relative_obj_path} due to regex {path_regex}")
                continue
            log.debug(f"Deleting {remote_obj.name}")
            self.bucket.delete_object(remote_obj)
            deleted_objects.append(remote_obj)
        return deleted_objects

    def list_objects(self, remote_path) -> List[RemoteObjectProtocol]:
        """
        :param remote_path: remote path on storage bucket relative to the configured remote base path.
        :return: list of remote objects under the remote path (multiple entries if the remote path is a directory)
        """
        full_remote_path = self._full_remote_path(remote_path)
        return self.bucket.list_objects(full_remote_path)
