import logging.handlers
import os
from copy import deepcopy
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Pattern, Protocol, Sequence, Union

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


class SyncedObject:
    def __init__(self, local_path, remote_obj: RemoteObjectProtocol = None):
        self.local_path = local_path
        self.exists_locally = os.path.isfile(local_path)
        self.local_size = os.path.getsize(local_path) if self.exists_locally else 0
        self.local_hash = md5sum(local_path) if self.exists_locally else None
        self.remote_obj = remote_obj

    @property
    def exists_on_target(self):
        return self.remote_obj or self.exists_locally

    @property
    def equal_md5_hash_sum(self):
        if self.exists_on_target:
            return self.local_size == self.remote_obj.size
        return False

    def execute_sync(self, storage: "RemoteStorage", direction: str) -> "SyncedObject":
        if direction == "push":
            remote_obj = storage.bucket.upload_object(
                self.local_path,
                storage.get_push_remote_path(self.local_path),
                verify_hash=False,
            )
            result = deepcopy(self)
            result.remote_obj = remote_obj
            return result

        elif direction == "pull":
            storage.pull_object(
                self.remote_obj,
                self.local_path,
                overwrite_existing=True,
            )
            result = SyncedObject(self.local_path, remote_obj=self.remote_obj)
            return result
        raise ValueError(
            f"Unknown direction {direction}, has to be either 'push' or 'pull'."
        )


# TODO
def _get_total_size(objects: Sequence[ObjectProtocol]):
    if len(objects) == 0:
        return 0
    return sum([obj.local_size for obj in objects])


@dataclass
class TransactionSummary:
    matched_files: List[SyncedObject] = field(default_factory=list)
    not_on_target: List[SyncedObject] = field(default_factory=list)
    on_target_eq_md5: List[SyncedObject] = field(default_factory=list)
    on_target_neq_md5: List[SyncedObject] = field(default_factory=list)
    unresolvable_collisions: Dict[str, List[RemoteObjectProtocol]] = field(
        default_factory=dict
    )
    skipped_files: List[SyncedObject] = field(default_factory=list)

    synced_files: List[SyncedObject] = field(default_factory=dict)

    @property
    def files_to_sync(self):
        return self.not_on_target + self.on_target_neq_md5

    def size_files_to_sync(self):
        return _get_total_size(self.files_to_sync)

    @property
    def requires_force(self):
        return len(self.on_target_neq_md5) != 0

    @property
    def has_unresolvable_collisions(self):
        return len(self.unresolvable_collisions) != 0

    @property
    def all_files_analyzed(self):
        return self.skipped_files + self.matched_files

    def add_entry(
        self,
        synced_object: Union[SyncedObject, str],
        collides_with: List[RemoteObjectProtocol] = None,
        matched=True,
    ):
        if isinstance(synced_object, str):
            synced_object = SyncedObject(synced_object)
        if not matched:
            self.skipped_files.append(synced_object)
            return

        if collides_with:
            self.unresolvable_collisions[synced_object.local_path] = collides_with

        if synced_object.exists_on_target:
            if synced_object.equal_md5_hash_sum:
                self.on_target_eq_md5.append(synced_object)
            else:
                self.on_target_neq_md5.append(synced_object)
        else:
            self.not_on_target.append(synced_object)

        self.matched_files.append(synced_object)


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

    def pull_object(
        self,
        remote_object: RemoteObjectProtocol,
        destination_path: str,
        overwrite_existing=False,
    ):
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
    ) -> List[ObjectProtocol]:
        r"""
        Pull either a file or a directory under the given path relative to local_base_dir.

        :param remote_path: remote path on storage bucket relative to the configured remote base path.
            e.g. 'data/ground_truth/some_file.json'
        :param local_base_dir: Local base directory for constructing local path
            e.g passing 'local_base_dir' will download to the path
            'local_base_dir/data/ground_truth/some_file.json' in the above example
        :param force: If False, pull will raise an error if an already existing file deviates from the remote in its md5sum.
            If True, these files are overwritten.
        :param path_regex: If not None only files with paths matching the regex will be pulled. This is useful for
            filtering files within a remote directory before pulling them.
        :param convert_to_linux_path: if True, will convert windows path to linux path (as needed by remote storage) and
            thus passing a remote path like 'data\my\path' will be converted to 'data/my/path' before pulling.
            This should only be set to False if you want to pull a remote object with '\' in its file name
            (which is discouraged).
        :param dryrun: If True, simulates the pull operation and returns the remote objects that would have been pulled.
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
            raise FileExistsError(
                f"Found {len(existing_neq_md5)} already existing files."
                "Set force=True to allow accsr to overwrite the files"
            )

        download_list = new_remote_objects + existing_neq_md5
        download_size = sum([obj.local_size for obj in download_list])

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
                self.pull_object(
                    remote_obj,
                    destination_path,
                    overwrite_existing=True,
                )
                downloaded_objects.append(remote_obj)
                pbar.update(remote_obj.local_size)

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
    ) -> Dict[str, List[RemoteObjectProtocol]]:
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
            if (obj.local_size == 0) or (
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
                if md5sum(destination_path) == obj.local_hash:
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

    def get_push_remote_path(self, local_path: str) -> str:
        """
        Get the full path within a remote storage bucket for pushing.

        :param local_path:
        :return:
        """
        return "/".join([self.remote_base_path, local_path]).replace(os.sep, "/")

    def get_push_summary(
        self,
        path: str,
        local_path_prefix: Optional[str] = None,
        path_regex: Pattern = None,
    ) -> TransactionSummary:
        """
        Retrieves the summary of the push operation

        :param path: Path to the local object (file or directory) to be uploaded, may be absolute or relative
        :param local_path_prefix: Prefix to be concatenated with ``path``
        :param path_regex: If not None only files with paths matching the regex will be pushed
        :return: A list of :class:`Object` instances for all remote objects that were created or matched existing files
        """
        summary = TransactionSummary()

        def check_match(file_path):
            if path_regex is None:
                return True
            if not path_regex.match(file_path):
                log.debug(
                    f"Skipping {file_path} since it does not match regular expression '{path_regex}'."
                )
                return False

        # collect all paths to scan
        local_path = self._get_push_local_path(path, local_path_prefix)
        if os.path.isfile(local_path):
            all_files_analyzed = [local_path]
        elif os.path.isdir(local_path):
            all_files_analyzed = []
            for root, _, fs in os.walk(local_path):
                all_files_analyzed.extend([os.path.join(root, f) for f in fs])
        else:
            raise FileNotFoundError(
                f"Local path {local_path} does not refer to a file or directory"
            )

        for file in tqdm(all_files_analyzed, desc="Scanning file: "):
            match = check_match(file)
            if not match:
                summary.add_entry(file, matched=False)
                continue

            collides_with = None
            remote_path = self.get_push_remote_path(file)
            matched_remote_obj = [
                obj
                for obj in self.bucket.list_objects(remote_path)
                if not self._listed_due_to_name_collision(remote_path, obj)
            ]

            remote_obj = None
            # name collision of local file with remote dir
            if len(matched_remote_obj) > 1:
                collides_with = matched_remote_obj

            elif matched_remote_obj:
                remote_obj = matched_remote_obj[0]

            synced_obj = SyncedObject(file, remote_obj)
            summary.add_entry(
                synced_obj,
                collides_with=collides_with,
            )

        return summary

    def push(
        self,
        path: str,
        local_path_prefix: Optional[str] = None,
        force=False,
        path_regex: Pattern = None,
        dryrun=False,
    ) -> TransactionSummary:
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
        :param force: If False, push will raise an error if an already existing remote file deviates from the local
            in its md5sum. If True, these files are overwritten.
        :param path_regex: If not None only files with paths matching the regex will be pushed
        :param dryrun: If True, simulates the push operation and returns the summary
            (with pushed_files being an empty list).
        :return: An object describing the summary of the operation.
        """
        summary = self.get_push_summary(path, local_path_prefix, path_regex)

        if dryrun:
            log.info(f"Skipping pull because dryrun=True")
            return summary

        if summary.has_unresolvable_collisions:
            raise FileExistsError(
                f"Found collisions of local files with remote directories, not pushing anything. "
                f"Affected files: {list(summary.unresolvable_collisions.keys())}. "
                f"Suggestion: perform a dryrun and analyze the push summary."
            )

        if summary.requires_force and not force:
            raise FileExistsError(
                f"The following files on remote would be overwritten but force=False: "
                f"{[f.local_path for f in summary.on_target_neq_md5]}. "
                f"Suggestion: perform a dryrun and analyze the push summary."
            )

        with tqdm(total=summary.size_files_to_sync(), desc="Progress (Bytes)") as pbar:
            for synced_object in summary.files_to_sync:
                pushed_obj = synced_object.execute_sync(self, direction="push")
                pbar.update(synced_object.local_size)
                summary.synced_files.append(pushed_obj)

        return summary

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
