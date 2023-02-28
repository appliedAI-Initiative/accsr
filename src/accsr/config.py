"""
Contains helpers for defining and providing configuration classes. A typical usage would be to create the files
*config.py*, *config.json* and *config_local.json* in a project's root directory. An example of a config.py for a
data-driven project is below. For a non-data driven project, the configuration class should inherit from
``accsr.config.ConfigurationBase``, and the resulting class will not have any pre-populated public entries.

>>> from accsr.config import DefaultDataConfiguration, ConfigProviderBase
>>>
>>> class __Configuration(DefaultDataConfiguration):
...     @property
...     def custom_entry_from_config(self):
...         return self._get_non_empty_entry("custom_entry_from_config")
...
...     @property
...     def existing_path_in_base_dir(self):
...         return self._get_existing_path(["base_dir", "path_in_base_dir"])
...
...     @property
...     def custom_path_in_processed_data(self):
...         return self.datafile_path("my_data", stage=self.PROCESSED, check_existence=False)
>>>
>>> class ConfigProvider(ConfigProviderBase[__Configuration]):
...     pass
>>>
>>> _config_provider = ConfigProvider()
>>>
>>>
>>> def get_config(reload=False):
...     return _config_provider.get_config(reload=reload)

"""


import inspect
import json
import logging.handlers
import os
from abc import ABC
from copy import deepcopy
from pathlib import Path
from typing import Callable, Dict, Generic, List, TextIO, Type, TypeVar, Union, get_args

log = logging.getLogger(__name__)


def recursive_dict_update(d: Dict, u: Dict):
    """
    Modifies d inplace by overwriting with non-dict values from u and updating all dict-values recursively.
    Returns the modified d.
    """
    # From https://stackoverflow.com/a/3233356/1069467
    for k, v in u.items():
        if isinstance(v, dict):
            d[k] = recursive_dict_update(d.get(k, {}), v)
        else:
            d[k] = v
    return d


def _replace_env_vars(conf: Union[dict], env_var_marker="env:"):
    for k, v in conf.items():
        if isinstance(v, str) and v.startswith(env_var_marker):
            env_var_name = v.lstrip(env_var_marker)
            conf[k] = os.getenv(env_var_name)
        elif isinstance(v, dict):
            _replace_env_vars(v, env_var_marker=env_var_marker)


def _get_entry_with_replaced_env_vars(
    entry: Union[str, float, list, dict], env_var_marker="env:"
):
    entry = deepcopy(entry)
    if isinstance(entry, str) and entry.startswith(env_var_marker):
        env_var_name = entry.lstrip(env_var_marker)
        return os.getenv(env_var_name)
    if isinstance(entry, dict):
        _replace_env_vars(entry, env_var_marker=env_var_marker)
        return entry
    if isinstance(entry, list):
        return [_get_entry_with_replaced_env_vars(v) for v in entry]
    return entry


def get_config_reader(filename: str) -> Callable[[TextIO], Dict]:
    """
    Returns a reader for yaml or json files. The file type is determined by the file extension.
    """
    if filename.endswith(".yaml") or filename.endswith(".yml"):
        import yaml

        return yaml.safe_load
    elif filename.endswith(".json"):
        return json.load
    raise ValueError(
        f"Unsupported file type for {filename}. Supported are .yaml, .yml and .json."
    )


class ConfigurationBase(ABC):
    """
    Base class for reading and retrieving configuration entries. Do not instantiate this class directly but
    instead inherit from it.
    """

    ENV_VAR_MARKER = "env:"

    def __init__(
        self,
        config_directory: str = None,
        config_files=("config.json", "config_local.json"),
    ):
        """
        :param config_directory: directory where to look for the config files. Typically, this will be a project's
            root directory. If None, the directory with the module containing the configuration class definition
            (inherited from ConfigurationBase) will be used.
        :param config_files: list of JSON or YAML configuration files (relative to config_directory) from which to read.
            The filenames should end in .json or .yaml/.yml.
            The configurations will be merged (dicts are merged, everything else is overwritten),
            entries more to the right have precedence.
            Non-existing files from the list will be ignored without errors or warnings. However, at least
            one file must exist for configuration to be read.
        """
        self.config_directory = (
            config_directory
            if config_directory is not None
            else self._module_dir_path()
        )
        self.config = {}
        for filename in config_files:
            file_path = os.path.join(self.config_directory, filename)
            file_reader = get_config_reader(filename)
            if os.path.exists(file_path):
                log.info(f"Reading configuration from {file_path}")
                with open(file_path, "r") as f:
                    read_config = file_reader(f)
                recursive_dict_update(self.config, read_config)
        if not self.config:
            raise FileNotFoundError(
                "No configuration entries could be read from"
                f"{[os.path.join(self.config_directory, c) for c in config_files]}"
            )

    def _module_dir_path(self):
        module_path = os.path.abspath(inspect.getfile(self.__class__))
        return os.path.dirname(module_path)

    def _get_non_empty_entry(
        self, key: Union[str, List[str]]
    ) -> Union[float, str, List, Dict]:
        """
        Retrieves an entry from the configuration

        :param key: key or list of keys to go through hierarchically
        :return: the queried json object
        """
        if isinstance(key, str):
            key = [key]
        value = self.config
        for k in key:
            value = value.get(k)
            if value is None:
                raise KeyError(f"Value for key '{key}' not set in configuration")
        return _get_entry_with_replaced_env_vars(value)

    def _get_existing_path(self, key: Union[str, List[str]], create=True) -> str:
        """
        Retrieves an existing local path from the configuration

        :param key: key or list of keys to go through hierarchically
        :param create: if True, a directory with the given path will be created on the fly.
        :return: the queried path
        """
        path_string = self._get_non_empty_entry(key)
        if os.path.isabs(path_string):
            path = path_string
        else:
            path = os.path.abspath(os.path.join(self.config_directory, path_string))
        if not os.path.exists(path):
            if isinstance(key, list):
                key = ".".join(key)  # purely for logging
            if create:
                log.info(
                    f"Configured directory {key}='{path}' not found; will create it"
                )
                os.makedirs(path)
            else:
                raise FileNotFoundError(
                    f"Configured directory {key}='{path}' does not exist."
                )
        return path.replace("/", os.sep)

    def _adjusted_path(self, path: str, relative: bool, check_existence: bool):
        """
        :param path:
        :param relative: If true, the returned path will be relative the project's top-level directory.
        :param check_existence: if True, will raise an error when file does not exist
        :return: the adjusted path, either absolute or relative
        """
        path = os.path.abspath(path)
        if check_existence and not os.path.exists(path):
            raise FileNotFoundError(f"No such file: {path}")
        if relative:
            return str(Path(path).relative_to(self.config_directory))
        return path


class DefaultDataConfiguration(ConfigurationBase, ABC):
    """
    Reads default configuration entries and contains retrieval methods for a typical data-driven project.
    A typical config.json file would look like this:

    | {
    |    "data_raw": "data/raw",
    |    "data_cleaned": "data/cleaned",
    |    "data_processed": "data/processed",
    |    "data_ground_truth": "data/ground_truth",
    |    "visualizations": "data/visualizations",
    |    "artifacts": "data/artifacts",
    |    "temp": "temp",
    |    "data": "data"
    | }

    """

    PROCESSED = "processed"
    RAW = "raw"
    CLEANED = "cleaned"
    GROUND_TRUTH = "ground_truth"
    DATA = "data"

    @property
    def artifacts(self):
        return self._get_existing_path("artifacts")

    @property
    def visualizations(self):
        return self._get_existing_path("visualizations")

    @property
    def temp(self):
        return self._get_existing_path("temp")

    @property
    def data(self):
        return self._get_existing_path("data")

    @property
    def data_raw(self):
        return self._get_existing_path("data_raw")

    @property
    def data_cleaned(self):
        return self._get_existing_path("data_cleaned")

    @property
    def data_processed(self):
        return self._get_existing_path("data_processed")

    @property
    def data_ground_truth(self):
        return self._get_existing_path("data_ground_truth")

    def datafile_path(
        self,
        filename: str,
        stage="raw",
        relative=False,
        check_existence=False,
    ):
        """
        :param filename:
        :param stage: raw, ground_truth, cleaned or processed
        :param relative: If True, the returned path will be relative the project's top-level directory
        :param check_existence: if True, will raise an error when file does not exist
        """
        basedir = self._data_basedir(stage)
        full_path = os.path.join(basedir, filename)
        return self._adjusted_path(full_path, relative, check_existence)

    def _data_basedir(self, stage):
        if stage == self.RAW:
            basedir = self.data_raw
        elif stage == self.CLEANED:
            basedir = self.data_cleaned
        elif stage == self.PROCESSED:
            basedir = self.data_processed
        elif stage == self.GROUND_TRUTH:
            basedir = self.data_ground_truth
        else:
            raise KeyError(f"Unknown stage: {stage}")
        return basedir

    def artifact_path(self, name: str, relative=False, check_existence=False):
        """
        :param name:
        :param relative: If true, the returned path will be relative the project's top-level directory.
        :param check_existence: if True, will raise an error when file does not exist
        :return:
        """
        full_path = os.path.join(self.artifacts, name)
        return self._adjusted_path(full_path, relative, check_existence)


ConfigurationClass = TypeVar("ConfigurationClass", bound=ConfigurationBase)


class ConfigProviderBase(Generic[ConfigurationClass], ABC):
    """
    Class for providing a config-singleton. Should not be instantiated directly but instead subclassed with an
    appropriate subclass of ConfigurationBase substituting the generic type.

    Usage example:
        >>> from accsr.config import ConfigurationBase, ConfigProviderBase
        >>> class __MyConfigClass(ConfigurationBase):
        ...     pass
        >>> class __MyConfigProvider(ConfigProviderBase[__MyConfigClass]):
        ...     pass
        ...
        >>> _config_provider = __MyConfigProvider()
        ...
        >>> def get_config():
        ...     return _config_provider.get_config()
    """

    def __init__(self):
        self.__config_instance = None
        self._config_args = None
        self._config_kwargs = None
        # retrieving the generic type at runtime, see
        # https://stackoverflow.com/questions/48572831/how-to-access-the-type-arguments-of-typing-generic
        self._config_constructor: Type[ConfigurationClass] = get_args(
            self.__class__.__orig_bases__[0]
        )[0]

    def _should_update_config_instance(self, reload: bool, args, kwargs):
        return (
            self.__config_instance is None
            or reload
            or self._config_args != args
            or self._config_kwargs != kwargs
        )

    def get_config(self, reload=False, *args, **kwargs) -> ConfigurationClass:
        """
        Retrieves the configuration object (as singleton).

        :param reload: if True, the config will be reloaded from disk even if a suitable
            configuration object already exists. This is mainly useful in interactive environments like notebooks.
        :param args: passed to init of the configuration class
        :param kwargs: passed to init of the configuration class constructor
        :return:
        """
        if self._should_update_config_instance(reload, args, kwargs):
            self._config_args = args
            self._config_kwargs = kwargs
            self.__config_instance = self._config_constructor(*args, **kwargs)
        return self.__config_instance
