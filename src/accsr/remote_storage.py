import json
import logging.handlers
import os
from copy import copy
from dataclasses import asdict, dataclass, field, is_dataclass
from enum import Enum
from functools import cached_property
from pathlib import Path
from typing import (
    Dict,
    List,
    Optional,
    Pattern,
    Protocol,
    Sequence,
    Union,
    runtime_checkable,
)

import libcloud
from libcloud.storage.base import Container, StorageDriver
from libcloud.storage.types import (
    ContainerAlreadyExistsError,
    InvalidContainerNameError,
)
from tqdm import tqdm

from accsr.files import md5sum

log = logging.getLogger(__name__)


class _SummariesJSONEncoder(json.JSONEncoder):
    def default(self, o):
        if is_dataclass(o):
            return asdict(o)
        if isinstance(o, RemoteObjectProtocol):
            return o.__dict__
        if isinstance(o, SyncObject):
            return o.to_dict()
        return str(o)


class _JsonReprMixin:
    def to_json(self) -> str:
        return json.dumps(self, indent=2, sort_keys=True, cls=_SummariesJSONEncoder)

    def __repr__(self):
        return f"\n{self.__class__.__name__}: \n{self.to_json()}\n"


class Provider(str, Enum):
    GOOGLE_STORAGE = "google_storage"
    S3 = "s3"


@runtime_checkable
class RemoteObjectProtocol(Protocol):
    """
    Protocol of classes that describe remote objects. Describes information about the remote object and functionality
    to download the object.
    """

    name: str
    size: int
    hash: int
    provider: str

    def download(
        self, download_path, overwrite_existing=False
    ) -> Optional["RemoteObjectProtocol"]:
        pass


class SyncObject(_JsonReprMixin):
    """
    Class representing the sync-status between a local path and a remote object. Is mainly used for creating
    summaries and syncing within RemoteStorage and for introspection before and after push/pull transactions.

    It is not recommended creating or manipulate instances of this class outside RemoteStorage, in particular
    in user code. This class forms part of the public interface because instances of it are given to users for
    introspection.
    """

    def __init__(
        self,
        local_path: str = None,
        remote_obj: RemoteObjectProtocol = None,
        remote_path: str = None,
    ):
        self.exists_locally = False
        self.local_path = None
        self.set_local_path(local_path)

        if self.local_path is None and remote_obj is None:
            raise ValueError(
                f"Either a local path or a remote object has to be passed."
            )

        self.remote_obj = remote_obj

        if remote_path is not None:
            if remote_obj is not None and remote_obj.name != remote_path:
                raise ValueError(
                    f"Passed both remote_path and remote_obj but the paths don't agree: "
                    f"{remote_path} != {remote_obj.name}"
                )
            self.remote_path = remote_path
        else:
            if remote_obj is None:
                raise ValueError(f"Either remote_path or remote_obj should be not None")
            self.remote_path = remote_obj.name

        self.local_size = os.path.getsize(local_path) if self.exists_locally else 0
        self.local_hash = md5sum(local_path) if self.exists_locally else None

    @property
    def name(self):
        return self.remote_path

    @property
    def exists_on_target(self) -> bool:
        """
        Getter of the exists_on_target property. Since the file exists at least once (locally or remotely), the property
        is True iff the file exists on both locations
        :return: True iff the file exists on both locations
        """
        return self.exists_on_remote and self.exists_locally

    def set_local_path(self, path: Optional[str]):
        """
        Changes the local path of the SyncObject
        :param path:
        :return: None
        """
        if path is not None:
            local_path = os.path.abspath(path)
            if os.path.isdir(local_path):
                raise FileExistsError(
                    f"local_path needs to point to file but pointed to a directory: {local_path}"
                )
            self.local_path = local_path
            self.exists_locally = os.path.isfile(local_path)

    @property
    def exists_on_remote(self):
        """
        Getter of the exists_on_remote property. Is true if the file was found on the remote.
        :return: whether the file exists on the remote
        """
        return self.remote_obj is not None

    @property
    def equal_md5_hash_sum(self):
        """
        Getter of the equal_md5_hash_sum property.
        :return: True if the local and the remote file have the same md5sum
        """
        if self.exists_on_target:
            return self.local_hash == self.remote_obj.hash
        return False

    def to_dict(self):
        result = copy(self.__dict__)
        result["exists_on_remote"] = self.exists_on_remote
        result["exists_on_target"] = self.exists_on_target
        result["equal_md5_hash_sum"] = self.equal_md5_hash_sum
        return result


def _get_total_size(objects: Sequence[SyncObject], mode="local"):
    """
    Computes the total size of the objects either on the local or on the remote side.
    :param objects: The SyncObjects for which the size should be computed
    :param mode: either 'local' or 'remote'
    :return: the total size of the objects on the specified side
    """
    permitted_modes = ["local", "remote"]
    if mode not in permitted_modes:
        raise ValueError(f"Unknown mode: {mode}. Has to be in {permitted_modes}.")
    if len(objects) == 0:
        return 0

    def get_size(obj: SyncObject):
        if mode == "local":
            if not obj.exists_locally:
                raise FileNotFoundError(
                    f"Cannot retrieve size of non-existing file: {obj.local_path}"
                )
            return obj.local_size
        if obj.remote_obj is None:
            raise FileNotFoundError(
                f"Cannot retrieve size of non-existing remote object corresponding to: {obj.local_path}"
            )
        return obj.remote_obj.size

    return sum([get_size(obj) for obj in objects])


@dataclass(repr=False)
class TransactionSummary(_JsonReprMixin):
    """
    Class representing the summary of a push or pull operation. Is mainly used for introspection before and after
    push/pull transactions.

    It is not recommended creating or manipulate instances of this class outside RemoteStorage, in particular
    in user code. This class forms part of the public interface because instances of it are given to users for
    introspection.
    """

    matched_source_files: List[SyncObject] = field(default_factory=list)
    not_on_target: List[SyncObject] = field(default_factory=list)
    on_target_eq_md5: List[SyncObject] = field(default_factory=list)
    on_target_neq_md5: List[SyncObject] = field(default_factory=list)
    unresolvable_collisions: Dict[str, Union[List[RemoteObjectProtocol], str]] = field(
        default_factory=dict
    )
    skipped_source_files: List[SyncObject] = field(default_factory=list)

    synced_files: List[SyncObject] = field(default_factory=list)
    sync_direction: Optional[str] = None

    def __post_init__(self):
        if self.sync_direction not in ["pull", "push", None]:
            raise ValueError(
                f"sync_direction can only be set to pull, push or None, instead got: {self.sync_direction}"
            )

    @property
    def files_to_sync(self) -> List[SyncObject]:
        """
        Returns of files that need synchronization.

        :return: list of all files that are not on the target or have different md5sums on target and remote
        """
        return self.not_on_target + self.on_target_neq_md5

    def size_files_to_sync(self) -> int:
        """
        Computes the total size of all objects that need synchronization. Raises a RuntimeError if the sync_direction
        property is not set to 'push' or 'pull'.

        :return: the total size of all local objects that need synchronization if self.sync_direction='push' and
            the size of all remote files that need synchronization if self.sync_direction='pull'
        """
        if self.sync_direction not in ["push", "pull"]:
            raise RuntimeError(
                "sync_direction has to be set to push or pull before computing sizes"
            )
        mode = "local" if self.sync_direction == "push" else "remote"
        return _get_total_size(self.files_to_sync, mode=mode)

    @property
    def requires_force(self) -> bool:
        """
        Getter of the requires_force property.
        :return: True iff a failure of the transaction can only be prevented by setting force=True.
        """
        return len(self.on_target_neq_md5) != 0

    @property
    def has_unresolvable_collisions(self) -> bool:
        """
        Getter of the requires_force property.
        :return: True iff there exists a collision that cannot be resolved.
        """
        return len(self.unresolvable_collisions) != 0

    @property
    def all_files_analyzed(self) -> List[SyncObject]:
        """
        Getter of the all_files_analyzed property.
        :return: list of all analyzed source files
        """
        return self.skipped_source_files + self.matched_source_files

    def add_entry(
        self,
        synced_object: Union[SyncObject, str],
        collides_with: Optional[Union[List[RemoteObjectProtocol], str]] = None,
        skip: bool = False,
    ):
        """
        Adds a SyncObject to the summary.
        :param synced_object: either a SyncObject or a path to a local file.
        :param collides_with: specification of unresolvable collisions for the given sync object
        :param skip: if True, the object is marked to be skipped
        :return: None
        """
        if isinstance(synced_object, str):
            synced_object = SyncObject(synced_object)
        if skip:
            self.skipped_source_files.append(synced_object)
            return

        self.matched_source_files.append(synced_object)
        if collides_with:
            self.unresolvable_collisions[synced_object.name] = collides_with
        elif synced_object.exists_on_target:
            if synced_object.equal_md5_hash_sum:
                self.on_target_eq_md5.append(synced_object)
            else:
                self.on_target_neq_md5.append(synced_object)
        else:
            self.not_on_target.append(synced_object)

    def get_short_summary_dict(self):
        """
        Returns a short summary of the transaction as a dictionary.
        """
        return {
            "sync_direction": self.sync_direction,
            "files_to_sync": len(self.files_to_sync),
            "total_size": self.size_files_to_sync(),
            "unresolvable_collisions": len(self.unresolvable_collisions),
            "synced_files": len(self.synced_files),
        }

    def print_short_summary(self):
        """
        Prints a short summary of the transaction (shorter than the full repr, which contains
        information about local and remote objects).
        """
        print(json.dumps(self.get_short_summary_dict(), indent=2))


@dataclass
class RemoteStorageConfig:
    """
    Contains all necessary information to establish a connection
    to a bucket within the remote storage, and the base path on the remote.
    """

    provider: str
    key: str
    bucket: str
    secret: str = field(repr=False)
    region: str = None
    host: str = None
    port: int = None
    base_path: str = ""
    secure: bool = True


class RemoteStorage:
    """
    Wrapper around lib-cloud for accessing remote storage services.
    :param conf:
    """

    def __init__(self, conf: RemoteStorageConfig):
        self._bucket: Optional[Container] = None
        self._conf = conf
        self._provider = conf.provider
        self._remote_base_path = ""
        self.set_remote_base_path(conf.base_path)
        possible_driver_kwargs = {
            "key": self.conf.key,
            "secret": self.conf.secret,
            "region": self.conf.region,
            "host": self.conf.host,
            "port": self.conf.port,
            "secure": self.conf.secure,
        }
        self.driver_kwargs = {
            k: v for k, v in possible_driver_kwargs.items() if v is not None
        }

    def create_bucket(self, exist_ok: bool = True):
        try:
            log.info(
                f"Creating bucket {self.conf.bucket} from configuration {self.conf}."
            )
            self.driver.create_container(container_name=self.conf.bucket)
        except (ContainerAlreadyExistsError, InvalidContainerNameError):
            if not exist_ok:
                raise
            log.info(
                f"Bucket {self.conf.bucket} already exists (or the name was invalid)."
            )

    @property
    def conf(self) -> RemoteStorageConfig:
        return self._conf

    @property
    def provider(self) -> str:
        return self._provider

    @property
    def remote_base_path(self) -> str:
        return self._remote_base_path

    def set_remote_base_path(self, path: Optional[str]):
        """
        Changes the base path in the remote storage
        (overriding the base path extracted from RemoteStorageConfig during instantiation).
        Pull and push operations will only affect files within the remote base path.

        :param path: a path with linux-like separators
        """
        if path is None:
            path = ""
        else:
            # google storage pulling and listing does not work with paths starting with "/"
            path = path.strip().lstrip("/")
        self._remote_base_path = path.strip()

    @cached_property
    def bucket(self) -> Container:
        log.info(f"Establishing connection to bucket {self.conf.bucket}")
        return self.driver.get_container(self.conf.bucket)

    @cached_property
    def driver(self) -> StorageDriver:
        storage_driver_factory = libcloud.get_driver(
            libcloud.DriverType.STORAGE, self.provider
        )
        return storage_driver_factory(**self.driver_kwargs)

    def execute_sync(
        self, sync_object: SyncObject, direction: str, force=False
    ) -> SyncObject:
        """
        Synchronizes the local and the remote file in the given direction. Will raise an error if a file from the source
        would overwrite an already existing file on the target and force=False. In this case, no operations will be
        performed on the target.

        :param sync_object: instance of SyncObject that will be used as basis for synchronization. Usually
            created from a get_*_summary method.
        :param direction: either "push" or "pull"
        :param force: if True, all already existing files on the target (with a different md5sum than the source files)
            will be overwritten.
        :return: a SyncObject that represents the status of remote and target after the synchronization
        """
        if direction not in ["push", "pull"]:
            raise ValueError(
                f"Unknown direction {direction}, has to be either 'push' or 'pull'."
            )
        if sync_object.equal_md5_hash_sum:
            log.debug(
                f"Skipping {direction} of {sync_object.name} because of coinciding hash sums"
            )
            return sync_object

        if sync_object.exists_on_target and not force:
            raise ValueError(
                f"Cannot perform {direction} because {sync_object.name} already exists and force is False"
            )

        if direction == "push":
            if not sync_object.exists_locally:
                raise FileNotFoundError(
                    f"Cannot push non-existing file: {sync_object.local_path}"
                )
            remote_obj = self.bucket.upload_object(
                sync_object.local_path,
                sync_object.remote_path,
                verify_hash=False,
            )
            return SyncObject(sync_object.local_path, remote_obj)

        elif direction == "pull":
            if None in [sync_object.remote_obj, sync_object.local_path]:
                raise RuntimeError(
                    f"Cannot pull without remote object and local path. Affects: {sync_object.name}"
                )
            if os.path.isdir(sync_object.local_path):
                raise FileExistsError(
                    f"Cannot pull file to a path which is an existing directory: {sync_object.local_path}"
                )

            log.debug(f"Fetching {sync_object.remote_obj.name} from {self.bucket.name}")
            os.makedirs(os.path.dirname(sync_object.local_path), exist_ok=True)
            sync_object.remote_obj.download(
                sync_object.local_path, overwrite_existing=force
            )
            return SyncObject(sync_object.local_path, sync_object.remote_obj)

    @staticmethod
    def _get_remote_path(remote_obj: RemoteObjectProtocol) -> str:
        """
        Returns the full path to the remote object. The resulting path never starts with "/" as it can cause problems
        with some backends (e.g. google cloud storage).
        """
        return remote_obj.name.lstrip("/")

    def _get_relative_remote_path(self, remote_obj: RemoteObjectProtocol) -> str:
        """
        Returns the path to the remote object relative to configured base dir (as expected by pull for a single file)
        """
        result = remote_obj.name
        result = result[len(self.remote_base_path) :]
        result = result.lstrip("/")
        return result

    def _full_remote_path(self, remote_path: str) -> str:
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
    ) -> bool:
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

    def _execute_sync_from_summary(
        self, summary: TransactionSummary, dryrun: bool = False, force: bool = False
    ) -> TransactionSummary:
        """
        Executes a transaction summary.
        :param summary: The transaction summary
        :param dryrun: if True, logs any error that would have prevented the execution and returns the summary
            without actually executing the sync.
        :param force: raises an error if dryrun=False and any files would be overwritten by the sync
        :return: Returns the input transaction summary. Note that the function potentially alters the state of the
            input summary.
        """
        if dryrun:
            log.info(f"Skipping {summary.sync_direction} because dryrun=True")
            if summary.has_unresolvable_collisions:
                log.warning(
                    f"This transaction has unresolvable collisions and would not succeed."
                )
            if summary.requires_force and not force:
                log.warning(
                    f"This transaction requires overwriting of files and would not succeed without force=True"
                )
            return summary

        if summary.has_unresolvable_collisions:
            raise FileExistsError(
                f"Found name collisions files with directories, not syncing anything. "
                f"Suggestion: perform a dryrun and analyze the summary. "
                f"Affected names: {list(summary.unresolvable_collisions.keys())}. "
            )

        if summary.requires_force and not force:
            raise FileExistsError(
                f"Operation requires overwriting of objects but force=False"
                f"Suggestion: perform a dryrun and analyze the summary. "
                f"Affected names: {[obj.name for obj in summary.on_target_neq_md5]}. "
            )

        with tqdm(total=summary.size_files_to_sync(), desc="Progress (Bytes)") as pbar:
            for sync_obj in summary.files_to_sync:
                synced_obj = self.execute_sync(
                    sync_obj, direction=summary.sync_direction, force=force
                )
                pbar.update(synced_obj.local_size)
                summary.synced_files.append(synced_obj)
        return summary

    def pull(
        self,
        remote_path: str,
        local_base_dir="",
        force=False,
        path_regex: Pattern = None,
        convert_to_linux_path=True,
        dryrun=False,
    ) -> TransactionSummary:
        r"""
        Pull either a file or a directory under the given path relative to local_base_dir.

        :param remote_path: remote path on storage bucket relative to the configured remote base path.
            e.g. 'data/ground_truth/some_file.json'
        :param local_base_dir: Local base directory for constructing local path
            e.g passing 'local_base_dir' will download to the path
            'local_base_dir/data/ground_truth/some_file.json' in the above example
        :param force: If False, pull will raise an error if an already existing file deviates from the remote in
            its md5sum. If True, these files are overwritten.
        :param path_regex: If not None only files with paths matching the regex will be pulled. This is useful for
            filtering files within a remote directory before pulling them.
        :param convert_to_linux_path: if True, will convert windows path to linux path (as needed by remote storage) and
            thus passing a remote path like 'data\my\path' will be converted to 'data/my/path' before pulling.
            This should only be set to False if you want to pull a remote object with '\' in its file name
            (which is discouraged).
        :param dryrun: If True, simulates the pull operation and returns the remote objects that would have been pulled.
        :return: An object describing the summary of the operation.
        """
        summary = self.get_pull_summary(
            remote_path,
            local_base_dir,
            path_regex,
            convert_to_linux_path=convert_to_linux_path,
        )
        if len(summary.all_files_analyzed) == 0:
            log.warning(f"No files found in remote storage under path: {remote_path}")
        return self._execute_sync_from_summary(summary, dryrun=dryrun, force=force)

    def _get_destination_path(
        self, obj: RemoteObjectProtocol, local_base_dir: str
    ) -> str:
        """
        Return the destination path of the given object
        """
        relative_obj_path = self._get_relative_remote_path(obj)
        return os.path.join(local_base_dir, relative_obj_path)

    def get_pull_summary(
        self,
        remote_path: str,
        local_base_dir="",
        path_regex: Pattern = None,
        convert_to_linux_path=True,
    ) -> TransactionSummary:
        r"""
        Creates TransactionSummary of the specified pull operation.

        :param remote_path: remote path on storage bucket relative to the configured remote base path.
            e.g. 'data/ground_truth/some_file.json'
        :param local_base_dir: Local base directory for constructing local path.
            Example: passing 'local_base_dir' will download to the path
            'local_base_dir/data/ground_truth/some_file.json' in the above example
        :param path_regex: If not None only files with paths matching the regex will be pulled. This is useful for
            filtering files within a remote directory before pulling them.
        :param convert_to_linux_path: if True, will convert windows path to linux path (as needed by remote storage) and
            thus passing a remote path like 'data\my\path' will be converted to 'data/my/path' before pulling.
            This should only be set to False if you want to pull a remote object with '\' in its file name
            (which is discouraged).
        :return:
        """
        local_base_dir = os.path.abspath(local_base_dir)
        if convert_to_linux_path:
            remote_path = remote_path.replace("\\", "/")

        summary = TransactionSummary(sync_direction="pull")
        full_remote_path = self._full_remote_path(remote_path)
        remote_objects: List[RemoteObjectProtocol] = list(
            self.bucket.list_objects(full_remote_path)
        )

        for obj in tqdm(remote_objects, desc="Remote paths: "):
            local_path = None
            collides_with = None
            skip = False
            if (obj.size == 0) or (
                self._listed_due_to_name_collision(full_remote_path, obj)
            ):
                log.debug(
                    f"Skipping {obj.name} since it was listed due to name collisions"
                )
                skip = True
            elif path_regex is not None:
                relative_obj_path = self._get_relative_remote_path(obj)
                if not path_regex.match(relative_obj_path):
                    log.debug(f"Skipping {relative_obj_path} due to regex {path_regex}")
                    skip = True

            if not skip:
                local_path = self._get_destination_path(obj, local_base_dir)
                if os.path.isdir(local_path):
                    collides_with = local_path

            summary.add_entry(
                SyncObject(local_path, obj), skip=skip, collides_with=collides_with
            )

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
        :return: the full local path of the file
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

        :param local_path: the local path to the file
        :return: the remote path that corresponds to the local path
        """
        return "/".join([self.remote_base_path, local_path]).replace(os.sep, "/")

    def get_push_summary(
        self,
        path: str,
        local_path_prefix: Optional[str] = None,
        path_regex: Optional[Pattern] = None,
    ) -> TransactionSummary:
        """
        Retrieves the summary of the push-transaction plan, before it has been executed.
        Nothing will be pushed and the synced_files entry of the summary will be an empty list.

        :param path: Path to the local object (file or directory) to be uploaded, may be absolute or relative
        :param local_path_prefix: Prefix to be concatenated with ``path``
        :param path_regex: If not None only files with paths matching the regex will be pushed
        :return: the summary object
        """
        summary = TransactionSummary(sync_direction="push")

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
            skip = False
            collides_with = None
            remote_obj = None
            if path_regex is not None and not path_regex.match(file):
                log.debug(
                    f"Skipping {file} since it does not match regular expression '{path_regex}'."
                )
                skip = True

            remote_path = self.get_push_remote_path(file)
            matched_remote_obj = [
                obj
                for obj in self.bucket.list_objects(remote_path)
                if not self._listed_due_to_name_collision(remote_path, obj)
            ]

            # name collision of local file with remote dir
            if len(matched_remote_obj) > 1:
                collides_with = matched_remote_obj

            elif matched_remote_obj:
                remote_obj = matched_remote_obj[0]

            synced_obj = SyncObject(file, remote_obj, remote_path=remote_path)
            summary.add_entry(
                synced_obj,
                collides_with=collides_with,
                skip=skip,
            )

        return summary

    def push(
        self,
        path: str,
        local_path_prefix: Optional[str] = None,
        force: bool = False,
        path_regex: Pattern = None,
        dryrun: bool = False,
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
            (with synced_files being an empty list). Same as get_push_summary method.
        :return: An object describing the summary of the operation.
        """
        summary = self.get_push_summary(path, local_path_prefix, path_regex)
        return self._execute_sync_from_summary(summary, dryrun=dryrun, force=force)

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

    def list_objects(self, remote_path: str) -> List[RemoteObjectProtocol]:
        """
        :param remote_path: remote path on storage bucket relative to the configured remote base path.
        :return: list of remote objects under the remote path (multiple entries if the remote path is a directory)
        """
        full_remote_path = self._full_remote_path(remote_path)
        return self.bucket.list_objects(full_remote_path)
