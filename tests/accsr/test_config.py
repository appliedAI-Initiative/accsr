import os

import pytest

from accsr.config import ConfigProviderBase, DefaultDataConfiguration
from accsr.remote_storage import RemoteStorageConfig


class __Configuration(DefaultDataConfiguration):
    # has to be kept in sync with resources/config.json and resources/config_local.yml
    @property
    def env_var_entry(self):
        return self._get_non_empty_entry("env_var_entry")

    @property
    def empty_env_var_entry(self):
        return self._get_non_empty_entry("empty_env_var_entry")

    @property
    def env_in_dict(self):
        return self._get_non_empty_entry(["some_dict", "env_in_dict"])

    @property
    def some_dict(self):
        return self._get_non_empty_entry("some_dict")

    @property
    def some_list(self):
        return self._get_non_empty_entry("some_list")

    @property
    def new_entry(self):
        return self._get_non_empty_entry("new_entry")

    @property
    def overwritten_entry(self):
        return self._get_non_empty_entry("overwritten_entry")


class ConfigProvider(ConfigProviderBase[__Configuration]):
    pass


_config_provider = ConfigProvider()


@pytest.fixture()
def test_config(test_resources, reload=False):
    return _config_provider.get_config(
        reload=reload,
        config_directory=test_resources,
        config_files=["config.json", "config_local.yml"],
    )


def test_storage_config_repr_does_not_include_secret():
    """
    Ensure that str representation of storage config does not leak secret.

    Regression test for issue #6.
    """
    cfg = RemoteStorageConfig(
        "provider", "key", "bucket", "secretkey", "region", "host", 1234, "base_path"
    )

    assert cfg.secret not in repr(cfg)
    assert cfg.secret not in str(cfg)


class TestConfig:
    def test_env_var_retrieval_at_runtime(self, test_config):
        os.environ["THIS_EXISTS"] = "env_entry"
        assert test_config.env_var_entry == "env_entry"
        assert test_config.env_in_dict == "env_entry"

        os.environ["THIS_EXISTS"] = "env_entry_new"
        assert test_config.env_var_entry == "env_entry_new"
        assert test_config.env_in_dict == "env_entry_new"

    def test_nested_env_vars_replaced_at_runtime(self, test_config):
        os.environ["THIS_EXISTS"] = "env_entry_in_dict"
        retrieved_dict = test_config.some_dict
        assert retrieved_dict["env_in_dict"] == "env_entry_in_dict"

        os.environ["THIS_EXISTS"] = "env_entry_in_dict_new"
        retrieved_dict = test_config.some_dict
        assert retrieved_dict["env_in_dict"] == "env_entry_in_dict_new"

    def test_env_vars_replaced_in_list(self, test_config):
        os.environ["THIS_EXISTS"] = "env_entry_in_list"
        retrieved_list = test_config.some_list
        assert retrieved_list[0] == "env_entry_in_list"

    def test_env_vars_NOT_replaced_in_pure_config(self, test_config):
        pure_config = test_config.config
        assert pure_config["env_var_entry"] == "env:THIS_EXISTS"

    def test_empty_env_gives_none(self, test_config):
        os.environ.pop("THIS_EXISTS_NOT", None)
        assert test_config.empty_env_var_entry is None

    def test_overwritten_entry(self, test_config):
        assert test_config.overwritten_entry == "overwritten"

    def test_new_entry(self, test_config):
        assert test_config.new_entry == "new"
