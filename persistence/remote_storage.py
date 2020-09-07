import logging.handlers
import os
from pathlib import Path
from typing import Optional, List

import libcloud
from libcloud.storage.base import StorageDriver, Container, Object
from libcloud.storage.types import ObjectDoesNotExistError

from config import get_config, RemoteStorageConfig

log = logging.getLogger(__name__)


class RemoteStorage:
    """
    Wrapper around lib-cloud for accessing remote storage services.

    :param conf:
    """

    def __init__(self, conf: RemoteStorageConfig):
        self._bucket: Optional[Container] = None
        self.conf = conf
        self.remote_base_path = conf.base_path

    @property
    def bucket(self):
        return self._maybe_instantiate_bucket()

    def _maybe_instantiate_bucket(self):
        if self._bucket is None:
            log.info(f"Establishing connection to bucket {self.conf.bucket}")
            storage_driver_factory = libcloud.get_driver(
                libcloud.DriverType.STORAGE, self.conf.provider
            )
            driver: StorageDriver = storage_driver_factory(
                key=self.conf.key, secret=self.conf.secret
            )
            self._bucket: Container = driver.get_container(self.conf.bucket)
        return self._bucket

    def pull_file(
        self, remote_path: str, local_base_dir=None, overwrite_existing=False
    ) -> Optional[Object]:
        """
        Pull a file from remote storage. If a file with the same name already exists locally,
        will not download anything unless overwrite_existing is True
        TODO: Change example paths if extracting this method to library
        :param path: remote path on storage bucket relative to the configured remote base path.
            e.g. 'data/ground_truth/full_workflow/nigeria-aoi1-labels.geojson'
        :param local_base_dir: Local base directory for constructing local path
            e.g 'tfe_vida_data' yields the path
            'tfe_vida_data/data/ground_truth/full_workflow/nigeria-aoi1-labels.geojson' in the above example
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
        log.info(f"Fetching {remote_path} from {self.bucket.name}")
        remote_object = self.bucket.get_object(remote_path)
        os.makedirs(os.path.dirname(download_path), exist_ok=True)
        remote_object.download(download_path, overwrite_existing=overwrite_existing)
        log.info(f"Downloaded {remote_path} to {os.path.abspath(download_path)}")
        return remote_object

    def pull_directory(
        self, remote_dir, local_base_dir: str = None, overwrite_existing=False
    ) -> List[Object]:
        """
        Pull all files from remote directory (including all subdirectories) to local_base_dir. Files with the same name
        as locally already existing ones will not be downloaded anything unless overwrite_existing is True
        TODO: Change example paths if extracting this method to library
        :param remote_dir:
        :param path: remote path relative to the configured remote base path.
            e.g. 'data/ground_truth/full_workflow'
        :param local_base_dir: Local base directory for constructing local path
            e.g 'tfe_vida_data' yields a path
            'tfe_vida_data/data/ground_truth/full_workflow' in the above example
        :param overwrite_existing: Overwrite directory if exists locally
        :return: list of :class:`Object` instances referring to all downloaded files
        """
        remote_path_prefix_len = len(self.remote_base_path) + 1
        remote_objects = list(
            self.bucket.list_objects("/".join([self.remote_base_path, remote_dir]))
        )
        if len(remote_objects) == 0:
            log.warning(f"No such remote directory: {remote_dir}. Not pulling anything")
            return []
        downloaded_objects = []
        for remote_obj in remote_objects:
            # removing the remote prefix from the full path
            path = remote_obj.name[remote_path_prefix_len:]
            downloaded_object = self.pull_file(
                path,
                local_base_dir=local_base_dir,
                overwrite_existing=overwrite_existing,
            )
            if downloaded_object is not None:
                downloaded_objects.append(downloaded_object)
        return downloaded_objects

    def pull(self, path: str, local_base_dir=None, overwrite_existing=False):
        """
        Pull either a file or a directory under the given path relative to local_base_dir. Files with the same name
        as locally already existing ones will not be downloaded anything unless overwrite_existing is True
        TODO: Change example paths if extracting this method to library
        :param path: remote path on storage bucket relative to the configured remote base path.
            e.g. 'data/ground_truth/full_workflow/nigeria-aoi1-labels.geojson'
        :param local_base_dir: Local base directory for constructing local path
            e.g 'tfe_vida_data' yields a path
            'tfe_vida_data/data/ground_truth/full_workflow/nigeria-aoi1-labels.geojson' in the above example
        :param overwrite_existing: Overwrite file if exists locally
        :return: list of :class:`Object` instances referring to all downloaded files
        """
        # directories in the remote storage are not objects, empty directories cannot exist
        # therefore, we can distinguish between directory and file by trying to pull it and checking for errors
        try:
            downloaded_object = self.pull_file(
                path,
                local_base_dir=local_base_dir,
                overwrite_existing=overwrite_existing,
            )
            if downloaded_object is not None:
                return [downloaded_object]
            return []
        except ObjectDoesNotExistError:
            return self.pull_directory(
                path,
                local_base_dir=local_base_dir,
                overwrite_existing=overwrite_existing,
            )

    def push_directory(
        self,
        path: str,
        local_path_prefix: Optional[str] = None,
        overwrite_existing=True,
    ) -> List[Object]:
        """
        Upload a directory from the given local path into the remote storage.

        Note that ``path`` may not be absolute if ``local_path_prefix`` is specified.

        :param path: Path to the local directory to be uploaded, may be absolute or relative
        :param local_path_prefix: Optional prefix for the local path
        :param overwrite_existing: If a remote object already exists, overwrite it?
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
                log.debug(f"Upload: {file=}, {root_path=}")
                obj = self.push_file(
                    file, root_path, overwrite_existing=overwrite_existing
                )
                objects.append(obj)
        return objects

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

    def push_file(
        self,
        path: str,
        local_path_prefix: Optional[str] = None,
        overwrite_existing=True,
    ) -> Object:
        """
        Upload a local file into the remote storage.

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

        log.debug(f"Uploading: {local_path} --> {remote_path}")
        remote_obj = self.bucket.upload_object(local_path, remote_path)
        return remote_obj

    def push(
        self,
        path: str,
        local_path_prefix: Optional[str] = None,
        overwrite_existing=True,
    ) -> List[Object]:
        """
        Upload a local file or directory into the remote storage.

        Note that ``path`` may not be absolute if ``local_path_prefix`` is specified.

        Usage examples::

            push("/foo/bar/baz.txt") --> <remote_base_path>/foo/bar/baz.txt
            push("foo/bar/baz.txt") --> <remote_base_path>/foo/bar/baz.txt
            push("bar/baz.txt", local_base_dir="/foo") --> <remote_base_path>/bar/baz.txt
            push("/absolute/path", local_base_dir="/something") --> ValueError

        :param path: Path to the local object (file or directory) to be uploaded, may be absolute or relative
        :param local_path_prefix: Prefix to be concatenated with ``path``
        :param overwrite_existing: If a remote object already exists, overwrite it?
        :return:
        """
        local_path = self._get_push_local_path(path, local_path_prefix)
        if os.path.isfile(local_path):
            return [self.push_file(path, local_path_prefix, overwrite_existing)]
        elif os.path.isdir(local_path):
            return self.push_directory(path, local_path_prefix, overwrite_existing)
        else:
            raise FileNotFoundError(
                f"Local path {local_path} does not refer to a file or directory"
            )


__default_remote_storage = None


def get_default_remote_storage():
    global __default_remote_storage
    c = get_config()
    if __default_remote_storage is None:
        __default_remote_storage = RemoteStorage(c.remote_storage)
    return __default_remote_storage
