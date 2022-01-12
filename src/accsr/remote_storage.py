import logging.handlers
import os
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import List, Optional, Pattern, Protocol, Tuple, Union

import libcloud
from libcloud.storage.base import Container, StorageDriver
from tqdm import tqdm

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


class LocalObject(RemoteObjectProtocol):
    def download(
        self, download_path, overwrite_existing=False
    ) -> Optional["RemoteObjectProtocol"]:
        return None

    def __init__(self, path):
        self.name = path
        self.size = os.path.getsize(path)
        self.hash = md5sum(path)
        self.provider = "local"


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
    ) -> None:
        """
        Download the remote object to the destination path.
        """

        destination_path = os.path.abspath(destination_path)
        if os.path.isdir(destination_path):
            raise FileExistsError(
                f"Cannot pull file to a path which is an existing directory: {destination_path}"
            )

        log.debug(f"Fetching {remote_object.name} from {self.bucket.name}")
        os.makedirs(os.path.dirname(destination_path), exist_ok=True)
        remote_object.download(destination_path, overwrite_existing=overwrite_existing)

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
        force=False,
        path_regex: Pattern = None,
        convert_to_linux_path=True,
        dryrun=False,
    ) -> List[RemoteObjectProtocol]:
        r"""
        Pull either a file or a directory under the given path relative to local_base_dir.

        :param remote_path: remote path on storage bucket relative to the configured remote base path.
            e.g. 'data/ground_truth/some_file.json'
        :param local_base_dir: Local base directory for constructing local path
            e.g passing 'local_base_dir' will download to the path
            'local_base_dir/data/ground_truth/some_file.json' in the above example
        :param force: If False pull will raise an error if an already existing file deviates from the remote in it's md5sum.
        If True these files are overwritten.
        :param path_regex: If not None only files with paths matching the regex will be pulled. This is useful for
            filtering files within a remote directory before pulling them.
        :param convert_to_linux_path: if True, will convert windows path to linux path (as needed by remote storage) and
            thus passing a remote path like 'data\my\path' will be converted to 'data/my/path' before pulling.
            This should only be set to False if you want to pull a remote object with '\' in its file name
            (which is discouraged).
        :param dryrun: If True simulates the pull operation and returns the remote objects that would have been pulled.
        :return: list of objects referring to all downloaded files
        """

        local_base_dir = os.path.abspath(local_base_dir)
        if convert_to_linux_path:
            remote_path = remote_path.replace("\\", "/")

        summary = self.collect_pull_summary(remote_path, local_base_dir, path_regex)

        new_remote_objects = summary["new_files"]
        existing_eq_md5 = summary["existing_eq_md5"]
        existing_neq_md5 = summary["existing_neq_md5"]
        blacklisted_remote_objects = summary["blacklisted"]

        if not force and len(existing_neq_md5) > 0:
            log.error(
                "Found existing files that would have been overwritten."
                "Set force=True to allow accsr to overwrite the files"
            )
            raise FileExistsError(
                f"Found {len(existing_neq_md5)} already existing files."
            )

        download_list = new_remote_objects + existing_neq_md5
        download_size = sum([obj.size for obj in download_list])

        if dryrun:
            log.info(
                f"""
                Pull summary:
                Download size: {download_size}
                # New Files: {len(new_remote_objects)}
                # Existing files with md5 hash identical to remote: {len(existing_eq_md5)} 
                # Existing files with md5 hash different from remote: {len(existing_neq_md5)}
                # Files excluded due to path_regex: {len(blacklisted_remote_objects)}
                """
            )
            return download_list

        if len(download_list) == 0:
            log.warning(f"Not pulling anything from remote path: {remote_path}.")
            return []

        # pull selected files
        downloaded_objects = []
        with tqdm(total=download_size, desc="Progress (Bytes)") as pbar:
            for remote_obj in new_remote_objects:
                destination_path = self._get_destination_path(
                    remote_obj, local_base_dir
                )
                self._pull_object(
                    remote_obj,
                    destination_path,
                    overwrite_existing=True,
                )
                downloaded_objects.append(remote_obj)
                pbar.update(remote_obj.size)

        return downloaded_objects

    def _get_destination_path(self, obj: RemoteObjectProtocol, local_base_dir):
        """
        Return the destination path of the given object
        """
        relative_obj_path = self._get_relative_remote_path(obj)
        return os.path.join(local_base_dir, relative_obj_path)

    def collect_pull_summary(
        self,
        remote_path: str,
        local_base_dir="",
        path_regex: Pattern = None,
    ) -> dict[str, List[LocalObject]]:
        """
        Creates a pull summary that contains
        - list of all remote files that do not exist locally
        - list of all remote files that already exist locally and have the same MD5 hash as the remote file
        - list of all remote files that already exist locally and have a different MD5 hash from the remote file
        :param remote_path: remote path on storage bucket relative to the configured remote base path.
            e.g. 'data/ground_truth/some_file.json'
        :param local_base_dir: Local base directory for constructing local path
            e.g passing 'local_base_dir' will download to the path
            'local_base_dir/data/ground_truth/some_file.json' in the above example
        :param path_regex: If not None only files with paths matching the regex will be pulled. This is useful for
            filtering files within a remote directory before pulling them.
        :return: pull summary as a dictionary
        """
        full_remote_path = self._full_remote_path(remote_path)
        remote_objects: List[RemoteObjectProtocol] = list(
            self.bucket.list_objects(full_remote_path)
        )

        valid_remote_objects = []
        existing_eq_md5 = []
        existing_neq_md5 = []
        blacklisted = []
        for obj in remote_objects:
            valid = True
            if (obj.size == 0) or (
                self._listed_due_to_name_collision(full_remote_path, obj)
            ):
                valid = False
            elif path_regex is not None:
                relative_obj_path = self._get_relative_remote_path(obj)
                if not path_regex.match(relative_obj_path):
                    log.info(f"Skipping {relative_obj_path} due to regex {path_regex}")
                    blacklisted.append(obj)
                    valid = False

            destination_path = self._get_destination_path(obj, local_base_dir)
            if os.path.isfile(destination_path):
                valid = False
                if md5sum(destination_path) == obj.hash:
                    existing_eq_md5.append(obj)
                else:
                    existing_neq_md5.append(obj)
            if valid:
                valid_remote_objects.append(obj)

        summary = {
            "new_files": valid_remote_objects,
            "existing_eq_md5": existing_eq_md5,
            "existing_neq_md5": existing_neq_md5,
            "blacklisted": blacklisted,
        }

        return summary

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

    def collect_push_summary(
        self,
        path: str,
        local_path_prefix: Optional[str] = None,
        path_regex: Pattern = None,
    ) -> dict[str, List[LocalObject]]:
        """
        Collect summary of the push operation

        :param path: Path to the local object (file or directory) to be uploaded, may be absolute or relative
        :param local_path_prefix: Prefix to be concatenated with ``path``
        :param path_regex: If not None only files with paths matching the regex will be pushed
        :return: A list of :class:`Object` instances for all remote objects that were created or matched existing files
        """
        local_path = self._get_push_local_path(path, local_path_prefix)
        if os.path.isfile(local_path):
            files = [local_path]
        elif os.path.isdir(local_path):
            files = []
            for root, _, fs in os.walk(local_path):
                files = files + [os.path.join(root, f) for f in fs]
        else:
            raise FileNotFoundError(
                f"Local path {local_path} does not refer to a file or directory"
            )

        if path_regex is not None and not path_regex.match(path):
            log.warning(
                f"{path} does not match regular expression '{path_regex}'. Nothing is pushed."
            )
            return {"new_files": [], "existing_eq_md5": [], "existing_neq_,d5": []}

        valid_files = []
        existing_eq_md5 = []
        existing_neq_md5 = []
        for file in files:
            valid = True
            remote_path = self._get_push_remote_path(file)
            remote_obj = [
                obj
                for obj in self.bucket.list_objects(remote_path)
                if not self._listed_due_to_name_collision(remote_path, obj)
            ]
            if len(remote_obj) > 1:
                valid = False

            if remote_obj:
                remote_obj = remote_obj[0]
                # Skip upload if MD5 hashes match
                if md5sum(local_path) == remote_obj.hash:
                    valid = False
                    existing_eq_md5.append(LocalObject(file))
                else:
                    valid = False
                    existing_neq_md5.append(LocalObject(file))

            if valid:
                valid_files.append(LocalObject(file))

        summary = {
            "new_files": valid_files,
            "existing_eq_md5": existing_eq_md5,
            "existing_neq_d5": existing_neq_md5,
        }

        return summary

    def push(
        self,
        path: str,
        local_path_prefix: Optional[str] = None,
        force=False,
        path_regex: Pattern = None,
        dryrun=False,
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

        :param path: Path to the local object (file or directory) to be uploaded, may be absolute or relative
        :param local_path_prefix: Prefix to be concatenated with ``path``
        :param force: If False push will raise an error if an already existing remote file deviates from the local
        in it's md5sum. If True these files are overwritten.
        :param path_regex: If not None only files with paths matching the regex will be pushed
        :param dryrun: If True simulates the pull operation and the returns local the objects that would have been pulled.
        :return: A list of :class:`Object` instances for all remote objects that were created or matched existing files
        """
        local_path = self._get_push_local_path(path, local_path_prefix)
        summary = self.collect_push_summary(path, local_path_prefix, path_regex)

        new_files = summary["new_files"]
        existing_eq_md5 = summary["existing_eq_md5"]
        existing_neq_md5 = summary["existing_neq_d5"]
        existing = existing_neq_md5 + existing_eq_md5

        if not force and len(existing_neq_md5) > 0:
            log.error(
                "Found existing files that would have been overwritten."
                "Set force=True to allow accsr to overwrite the files"
            )
            raise FileExistsError(
                f"Found {len(existing_neq_md5)} already existing file(s)."
            )

        upload_list = new_files + existing_neq_md5
        upload_size = sum([obj.size for obj in upload_list])

        if dryrun:
            log.info(
                f"""
                Push summary: 
                # Remote files: {len(new_files) + len(existing)}
                Upload size: {upload_size} ({len(upload_list)} files)
                Existing files with md5 hash identical to local: {len(existing_eq_md5)} 
                Existing files with md5 hash different from local: {len(existing_neq_md5)}
                """
            )
            return upload_list

        if len(upload_list) == 0:
            log.warning(f"Not pushing anything from local path: {local_path}.")
            return []

        # Upload selected files
        result = []
        with tqdm(total=upload_size, desc="Progress (Bytes)") as pbar:
            for file in new_files:
                obj = self.bucket.upload_object(
                    file.name, self._get_push_remote_path(file.name), verify_hash=False
                )
                result.append(obj)
                pbar.update(obj.size)

        return result

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
