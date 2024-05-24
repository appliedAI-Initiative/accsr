import glob
import json
import logging.handlers
import os
import re
from contextlib import contextmanager
from copy import copy
from dataclasses import asdict, dataclass, field, is_dataclass
from enum import Enum
from functools import cached_property
from pathlib import Path
from typing import (
    Dict,
    Generator,
    List,
    Literal,
    Optional,
    Pattern,
    Protocol,
    Sequence,
    Union,
    cast,
    runtime_checkable,
)

from libcloud.storage.base import Container, StorageDriver
from libcloud.storage.providers import get_driver
from libcloud.storage.types import (
    ContainerAlreadyExistsError,
    InvalidContainerNameError,
)
from tqdm import tqdm

from accsr.files import md5sum

log = logging.getLogger(__name__)


def _to_optional_pattern(regex: Optional[Union[str, Pattern]]) -> Optional[Pattern]:
    if isinstance(regex, str):
        return re.compile(regex)
    return regex


class _SummariesJSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, TransactionSummary):
            # special case for TransactionSummary, since the drivers are not serializable and dataclasses.asdict
            # calls deepcopy
            result = copy(o.__dict__)
            _replace_driver_by_name(result)
            return result
        if is_dataclass(o):
            return asdict(o)
        if isinstance(o, RemoteObjectProtocol):
            result = copy(o.__dict__)
            _replace_driver_by_name(result)
            return result
        if isinstance(o, SyncObject):
            return o.to_dict(make_serializable=True)
        return str(o)


def _replace_driver_by_name(obj):
    # The driver object from libcloud stores a connection and is not serializable.
    # Since sometimes we want to be able to deepcopy these things around,
    # we replace the driver by its name. This is needed for `asdict` to work.
    if isinstance(obj, RemoteObjectProtocol) and hasattr(obj, "driver"):
        obj.driver = obj.driver.name  # type: ignore
    if isinstance(obj, list) or isinstance(obj, tuple):
        for item in obj:
            _replace_driver_by_name(item)
    if isinstance(obj, dict):
        for key, value in obj.items():
            _replace_driver_by_name(value)


class _JsonReprMixin:
    def to_json(self) -> str:
        return json.dumps(self, indent=2, sort_keys=True, cls=_SummariesJSONEncoder)

    def __repr__(self):
        return f"\n{self.__class__.__name__}: \n{self.to_json()}\n"


@contextmanager
def _switch_to_dir(path: Optional[str] = None) -> Generator[None, None, None]:
    if path:
        cur_dir = os.getcwd()
        try:
            os.chdir(path)
            yield
        finally:
            os.chdir(cur_dir)
    else:
        yield


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
        local_path: Optional[str] = None,
        remote_obj: Optional[RemoteObjectProtocol] = None,
        remote_path: Optional[str] = None,
    ):
        if remote_path is not None:
            remote_path = remote_path.lstrip("/")
        if remote_obj is not None:
            remote_obj = copy(remote_obj)
            remote_obj.name = remote_obj.name.lstrip("/")

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

        if self.exists_locally:
            assert self.local_path is not None
            self.local_size = os.path.getsize(self.local_path)
            self.local_hash = md5sum(self.local_path)
        else:
            self.local_size = 0
            self.local_hash = None

    @property
    def name(self):
        return self.remote_path

    @property
    def exists_on_target(self) -> bool:
        """
        True iff the file exists on both locations
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
        return self.remote_obj is not None

    @property
    def equal_md5_hash_sum(self):
        if self.exists_on_target:
            return self.local_hash == self.remote_obj.hash
        return False

    def to_dict(self, make_serializable=True):
        result = copy(self.__dict__)
        if make_serializable:
            _replace_driver_by_name(result)

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
    sync_direction: Optional[Literal["push", "pull"]] = None

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
        else:
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
    region: Optional[str] = None
    host: Optional[str] = None
    port: Optional[int] = None
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
        storage_driver_factory = get_driver(self.provider)
        return storage_driver_factory(**self.driver_kwargs)

    def _execute_sync(
        self, sync_object: SyncObject, direction: Literal["push", "pull"], force=False
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
            assert sync_object.local_path is not None
            remote_obj = cast(
                RemoteObjectProtocol,
                self.bucket.upload_object(
                    sync_object.local_path,
                    sync_object.remote_path,
                    verify_hash=False,
                ),
            )
            return SyncObject(sync_object.local_path, remote_obj)

        elif direction == "pull":
            if None in [sync_object.remote_obj, sync_object.local_path]:
                raise RuntimeError(
                    f"Cannot pull without remote object and local path. Affects: {sync_object.name}"
                )
            assert sync_object.local_path is not None
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
        else:
            raise ValueError(
                f"Unknown direction {direction}, has to be either 'push' or 'pull'."
            )

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

        Example 1: full remote path is 'pull/this/dir' and remote storage includes paths like 'pull/this/dir_subfix'.
        Example 2: full remote path is 'delete/this/file' and remote storage includes paths like 'delete/this/file_2'.

        All such paths will be listed in bucket.list_objects(full_remote_path), and we need to exclude them in
        most methods like pull or delete.

        :param full_remote_path: usually the output of self._full_remote_path(remote_path)
        :param remote_object: the object to check
        :return:
        """
        # no name collisions possible in this case
        if full_remote_path.endswith("/") or full_remote_path == "":
            return False

        # Remove leading / for comparison of paths
        full_remote_path = full_remote_path.lstrip("/")
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
                f"Operation requires overwriting of objects but `force=False`. "
                f"Suggestion: perform a dryrun and analyze the summary. "
                f"Affected names: {[obj.name for obj in summary.on_target_neq_md5]}. "
            )

        desc = f"{summary.sync_direction}ing (bytes)"
        if force:
            desc = "force " + desc
        with tqdm(total=summary.size_files_to_sync(), desc=desc) as pbar:
            for sync_obj in summary.files_to_sync:
                assert summary.sync_direction is not None
                synced_obj = self._execute_sync(
                    sync_obj, direction=summary.sync_direction, force=force
                )
                pbar.update(synced_obj.local_size)
                summary.synced_files.append(synced_obj)
        return summary

    def pull(
        self,
        remote_path: str,
        local_base_dir: str = "",
        force: bool = False,
        include_regex: Optional[Union[Pattern, str]] = None,
        exclude_regex: Optional[Union[Pattern, str]] = None,
        convert_to_linux_path: bool = True,
        dryrun: bool = False,
        path_regex: Optional[Union[Pattern, str]] = None,
        strip_abspath_prefix: Optional[str] = None,
        strip_abs_local_base_dir: bool = True,
    ) -> TransactionSummary:
        r"""
        Pull either a file or a directory under the given path relative to local_base_dir.

        :param remote_path: remote path on storage bucket relative to the configured remote base path.
            e.g. 'data/ground_truth/some_file.json'. Can also be an absolute local path if ``strip_abspath_prefix``
            is specified.
        :param local_base_dir: Local base directory for constructing local path
            e.g. passing 'local_base_dir' will download to the path
            'local_base_dir/data/ground_truth/some_file.json' in the above example
        :param force: If False, pull will raise an error if an already existing file deviates from the remote in
            its md5sum. If True, these files are overwritten.
        :param include_regex: If not None only files with paths matching the regex will be pulled. This is useful for
            filtering files within a remote directory before pulling them.
        :param exclude_regex: If not None, files with paths matching the regex will be excluded from the pull.
            Takes precedence over ``include_regex``, i.e. if a file matches both, it will be excluded.
        :param convert_to_linux_path: if True, will convert windows path to linux path (as needed by remote storage) and
            thus passing a remote path like 'data\my\path' will be converted to 'data/my/path' before pulling.
            This should only be set to False if you want to pull a remote object with '\' in its file name
            (which is discouraged).
        :param dryrun: If True, simulates the pull operation and returns the remote objects that would have been pulled.
        :param path_regex: DEPRECATED! Use ``include_regex`` instead.
        :param strip_abspath_prefix: Will only have an effect if the `remote_path` is absolute.
            Then the given prefix is removed from it before pulling. This is useful for pulling files from a remote storage
            by directly specifying absolute local paths instead of first converting them to actual remote paths.
            Similar in logic to `local_path_prefix` in `push`.
            A common use case is to always set `local_base_dir` to the same value and to always pass absolute paths
            as `remote_path` to `pull`.
        :param strip_abs_local_base_dir: If True, and `local_base_dir` is an absolute path, then
            the `local_base_dir` will be treated as `strip_abspath_prefix`. See explanation of `strip_abspath_prefix`.
        :return: An object describing the summary of the operation.
        """

        if strip_abs_local_base_dir and os.path.isabs(local_base_dir):
            if strip_abspath_prefix is not None:
                raise ValueError(
                    f"Cannot specify both `strip_abs_local_base_dir`={strip_abs_local_base_dir} "
                    f"and `strip_abspath_prefix`={strip_abspath_prefix}"
                    f"when `local_base_dir`={local_base_dir} is an absolute path."
                )
            strip_abspath_prefix = local_base_dir

        remote_path_is_abs = remote_path.startswith("/") or os.path.isabs(remote_path)

        if strip_abspath_prefix is not None and remote_path_is_abs:
            remote_path = remote_path.replace("\\", "/")
            strip_abspath_prefix = strip_abspath_prefix.replace("\\", "/").rstrip("/")
            if not remote_path.startswith(strip_abspath_prefix):
                raise ValueError(
                    f"Remote path {remote_path} is absolute but does not start "
                    f"with the given prefix {strip_abspath_prefix}"
                )
            # +1 for removing the leading '/'
            remote_path = remote_path[len(strip_abspath_prefix) + 1 :]

        include_regex = self._handle_deprecated_path_regex(include_regex, path_regex)
        summary = self._get_pull_summary(
            remote_path,
            local_base_dir,
            include_regex=include_regex,
            exclude_regex=exclude_regex,
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

    def _get_pull_summary(
        self,
        remote_path: str,
        local_base_dir: str = "",
        include_regex: Optional[Union[Pattern, str]] = None,
        exclude_regex: Optional[Union[Pattern, str]] = None,
        convert_to_linux_path: bool = True,
        path_regex: Optional[Union[Pattern, str]] = None,
    ) -> TransactionSummary:
        r"""
        Creates TransactionSummary of the specified pull operation.

        :param remote_path: remote path on storage bucket relative to the configured remote base path.
            e.g. 'data/ground_truth/some_file.json'
        :param local_base_dir: Local base directory for constructing local path.
            Example: passing 'local_base_dir' will download to the path
            'local_base_dir/data/ground_truth/some_file.json' in the above example
        :param include_regex: If not None, only files with paths matching the regex will be pulled. This is useful for
            filtering files within a remote directory before pulling them.
        :param exclude_regex: If not None, only files with paths not matching the regex will be pulled.
           Takes precedence over include_regex, i.e. if a file matches both, it will be excluded.
        :param convert_to_linux_path: if True, will convert windows path to linux path (as needed by remote storage) and
            thus passing a remote path like 'data\my\path' will be converted to 'data/my/path' before pulling.
            This should only be set to False if you want to pull a remote object with '\' in its file name
            (which is discouraged).
        :param path_regex: DEPRECATED! use ``include_regex`` instead.
        :return:
        """
        include_regex = self._handle_deprecated_path_regex(include_regex, path_regex)

        include_regex = _to_optional_pattern(include_regex)
        exclude_regex = _to_optional_pattern(exclude_regex)

        local_base_dir = os.path.abspath(local_base_dir)
        if convert_to_linux_path:
            remote_path = remote_path.replace("\\", "/")

        summary = TransactionSummary(sync_direction="pull")
        full_remote_path = self._full_remote_path(remote_path)
        # noinspection PyTypeChecker
        remote_objects = cast(
            List[RemoteObjectProtocol], list(self.bucket.list_objects(full_remote_path))
        )

        for obj in tqdm(
            remote_objects,
            desc=f"Scanning remote paths in {self.bucket.name}/{full_remote_path}: ",
        ):
            local_path = None
            collides_with = None
            if (obj.size == 0) or (
                self._listed_due_to_name_collision(full_remote_path, obj)
            ):
                log.debug(
                    f"Skipping {obj.name} since it was listed due to name collisions"
                )
                skip = True
            else:
                relative_obj_path = self._get_relative_remote_path(obj)
                skip = self._should_skip(
                    relative_obj_path, include_regex, exclude_regex
                )

            if not skip:
                local_path = self._get_destination_path(obj, local_base_dir)
                if os.path.isdir(local_path):
                    collides_with = local_path

            summary.add_entry(
                SyncObject(local_path, obj), skip=skip, collides_with=collides_with
            )

        return summary

    def get_push_remote_path(self, local_path: str) -> str:
        """
        Get the full path within a remote storage bucket for pushing.

        :param local_path: the local path to the file
        :return: the remote path that corresponds to the local path
        """
        return (
            "/".join([self.remote_base_path, local_path])
            .replace(os.sep, "/")
            .lstrip("/")
        )

    def _get_push_summary(
        self,
        path: str,
        local_path_prefix: Optional[str] = None,
        include_regex: Optional[Union[Pattern, str]] = None,
        exclude_regex: Optional[Union[Pattern, str]] = None,
        path_regex: Optional[Union[Pattern, str]] = None,
    ) -> TransactionSummary:
        """
        Retrieves the summary of the push-transaction plan, before it has been executed.
        Nothing will be pushed and the synced_files entry of the summary will be an empty list.

        :param path: Path to the local object (file or directory) to be uploaded, may be absolute or relative.
            globs are permitted, thus ``path`` may contain wildcards.
        :param local_path_prefix: path names on the remote will be relative to this path. Thus, specifying
            for example ``local_path_prefix=/bar/foo`` (on a unix system) and ``path=baz``
            will push ``/bar/foo/baz`` to ``remote_base_path/baz``. The same will happen if
            ``path=/bar/foo/baz`` is specified.
            **NOTE**: if ``local_path_prefix`` is specified and ``path`` is absolute, it is assumed that
            ``path`` is child of ``local_path_prefix``. If this is not the case, an error will be raised.
        :param include_regex: If not None, only files with paths matching the regex will be pushed.
            Note that paths matched against the regex will be relative to ``local_path_prefix``.
        :param exclude_regex: If not None, only files with paths not matching the regex will be pushed.
            Takes precedence over ``include_regex``, i.e. if a file matches both regexes, it will be excluded.
            Note that paths matched against the regex will be relative to ``local_path_prefix``.
        :param path_regex: DEPRECATED! Same as ``include_regex``.
        :return: the summary object
        """
        summary = TransactionSummary(sync_direction="push")
        include_regex = self._handle_deprecated_path_regex(include_regex, path_regex)

        if local_path_prefix is not None:
            local_path_prefix = os.path.abspath(local_path_prefix)
        include_regex = _to_optional_pattern(include_regex)
        exclude_regex = _to_optional_pattern(exclude_regex)

        _path = Path(path)
        if _path.is_absolute() and local_path_prefix:
            try:
                path = str(_path.relative_to(local_path_prefix))
            except ValueError:
                raise ValueError(
                    f"Specified {path=} is not a child of {local_path_prefix=}"
                )

        # at this point, path is relative to local_path_prefix.
        with _switch_to_dir(local_path_prefix):
            # collect all paths to scan
            all_files_analyzed = []
            for local_path in glob.glob(path):
                if os.path.isfile(local_path):
                    all_files_analyzed.append(local_path)
                elif os.path.isdir(local_path):
                    for root, _, fs in os.walk(local_path):
                        all_files_analyzed.extend([os.path.join(root, f) for f in fs])
            if len(all_files_analyzed) == 0:
                raise FileNotFoundError(
                    f"No files found under {path=} with {local_path_prefix=}"
                )

            for file in tqdm(
                all_files_analyzed,
                desc=f"Scanning files in {os.path.join(os.getcwd(), path)}: ",
            ):
                collides_with = None
                remote_obj = None
                skip = self._should_skip(file, include_regex, exclude_regex)

                remote_path = self.get_push_remote_path(file)

                all_matched_remote_obj = cast(
                    List[RemoteObjectProtocol], self.bucket.list_objects(remote_path)
                )
                matched_remote_obj = [
                    obj
                    for obj in all_matched_remote_obj
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

    @staticmethod
    def _should_skip(
        file: str, include_regex: Optional[Pattern], exclude_regex: Optional[Pattern]
    ):
        if include_regex is not None and not include_regex.match(file):
            log.debug(
                f"Skipping {file} since it does not match regular expression '{include_regex}'."
            )
            return True
        if exclude_regex is not None and exclude_regex.match(file):
            log.debug(
                f"Skipping {file} since it matches regular expression '{exclude_regex}'."
            )
            return True
        return False

    @staticmethod
    def _handle_deprecated_path_regex(
        include_regex: Optional[Union[Pattern, str]],
        path_regex: Optional[Union[Pattern, str]],
    ):
        if path_regex is not None:
            log.warning(
                "Using deprecated parameter 'path_regex'. Use 'include_regex' instead."
            )
            if include_regex is not None:
                raise ValueError(
                    "Cannot specify both 'path_regex' and 'include_regex'. "
                    "Use only 'include_regex' instead, 'path_regex' is deprecated."
                    f"Got {path_regex=} and {include_regex=}"
                )
            include_regex = path_regex
        return include_regex

    def push(
        self,
        path: str,
        local_path_prefix: Optional[str] = None,
        force: bool = False,
        include_regex: Optional[Union[Pattern, str]] = None,
        exclude_regex: Optional[Union[Pattern, str]] = None,
        dryrun: bool = False,
        path_regex: Optional[Union[Pattern, str]] = None,
    ) -> TransactionSummary:
        """
        Upload files into the remote storage.
        Does not upload files for which the md5sum matches existing remote files.
        The remote path for uploading will be constructed from the remote_base_path and the provided path.
        The `local_path_prefix` serves for finding the directory on the local system or for stripping off
        parts of absolute paths if path is absolute, see examples below.

        Examples:
           1) path=foo/bar, local_path_prefix=None -->
                ./foo/bar uploaded to remote_base_path/foo/bar
           2) path=/home/foo/bar, local_path_prefix=None -->
                /home/foo/bar uploaded to remote_base_path/home/foo/bar
           3) path=bar, local_path_prefix=/home/foo -->
                /home/foo/bar uploaded to remote_base_path/bar
           4) path=/home/foo/bar, local_path_prefix=/home/foo -->
                /home/foo/bar uploaded to remote_base_path/bar (Same as 3)
           5) path=/home/baz/bar, local_path_prefix=/home/foo -->
                ValueError: Specified path=/home/baz/bar is not a child of local_path_prefix=/home/foo

        :param path: Path to the local object (file or directory) to be uploaded, may be absolute or relative.
            globs are supported as well, thus ``path`` may be a pattern like ``*.txt``.
        :param local_path_prefix: Prefix to be concatenated with ``path``
        :param force: If False, push will raise an error if an already existing remote file deviates from the local
            in its md5sum. If True, these files are overwritten.
        :param include_regex: If not None, only files with paths matching the regex will be pushed.
            Note that paths matched against the regex will be relative to ``local_path_prefix``.
        :param exclude_regex: If not None, only files with paths not matching the regex will be pushed. Takes precedence
            over ``include_regex``, i.e. if a file matches both regexes, it will be excluded.
            Note that paths matched against the regex will be relative to ``local_path_prefix``.
        :param dryrun: If True, simulates the push operation and returns the summary
            (with synced_files being an empty list).
        :param path_regex: DEPRECATED! Same as ``include_regex``.
        :return: An object describing the summary of the operation.
        """
        include_regex = self._handle_deprecated_path_regex(include_regex, path_regex)
        summary = self._get_push_summary(
            path,
            local_path_prefix,
            include_regex=include_regex,
            exclude_regex=exclude_regex,
        )
        return self._execute_sync_from_summary(summary, dryrun=dryrun, force=force)

    def delete(
        self,
        remote_path: str,
        include_regex: Optional[Union[Pattern, str]] = None,
        exclude_regex: Optional[Union[Pattern, str]] = None,
        path_regex: Optional[Union[Pattern, str]] = None,
    ) -> List[RemoteObjectProtocol]:
        """
        Deletes a file or a directory under the given path relative to local_base_dir. Use with caution!

        :param remote_path: remote path on storage bucket relative to the configured remote base path.
        :param include_regex: If not None only files with paths matching the regex will be deleted.
        :param exclude_regex: If not None only files with paths not matching the regex will be deleted.
            Takes precedence over ``include_regex``, i.e. if a file matches both regexes, it will be excluded.
        :param path_regex: DEPRECATED! Same as ``include_regex``.
        :return: list of remote objects referring to all deleted files
        """
        include_regex = self._handle_deprecated_path_regex(include_regex, path_regex)
        include_regex = _to_optional_pattern(include_regex)
        exclude_regex = _to_optional_pattern(exclude_regex)

        full_remote_path = self._full_remote_path(remote_path)

        remote_objects = cast(
            List[RemoteObjectProtocol], self.bucket.list_objects(full_remote_path)
        )
        if len(remote_objects) == 0:
            log.warning(
                f"No such remote file or directory: {full_remote_path}. Not deleting anything"
            )
            return []
        deleted_objects = []
        for remote_obj in remote_objects:
            if self._listed_due_to_name_collision(full_remote_path, remote_obj):
                log.debug(
                    f"Skipping deletion of {remote_obj.name} as it was listed due to name collision"
                )
                continue

            relative_obj_path = self._get_relative_remote_path(remote_obj)
            if include_regex is not None and not include_regex.match(relative_obj_path):
                log.info(f"Skipping {relative_obj_path} due to regex {include_regex}")
                continue
            if exclude_regex is not None and exclude_regex.match(relative_obj_path):
                log.info(f"Skipping {relative_obj_path} due to regex {exclude_regex}")
                continue
            log.debug(f"Deleting {remote_obj.name}")
            self.bucket.delete_object(remote_obj)  # type: ignore
            deleted_objects.append(remote_obj)
        return deleted_objects

    def list_objects(self, remote_path: str) -> List[RemoteObjectProtocol]:
        """
        :param remote_path: remote path on storage bucket relative to the configured remote base path.
        :return: list of remote objects under the remote path (multiple entries if the remote path is a directory)
        """
        full_remote_path = self._full_remote_path(remote_path)
        return self.bucket.list_objects(full_remote_path)  # type: ignore
