import os

import pytest

from accsr.config import ConfigProviderBase, DefaultDataConfiguration
from accsr.remote_storage import RemoteStorageConfig


class __Configuration(DefaultDataConfiguration):
    # has to be kept in sync with resources/config.json
    @property
    def env_var_entry(self):
        return self._get_non_empty_entry("env_var_entry")

    @property
    def empty_env_var_entry(self):
        return self._get_non_empty_entry("empty_env_var_entry")


class ConfigProvider(ConfigProviderBase[__Configuration]):
    pass


_config_provider = ConfigProvider()


@pytest.fixture()
def test_config(test_resources, reload=False):
    return _config_provider.get_config(reload=reload, config_directory=test_resources)


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
    def test_env_var_retrieval(self, test_config):
        os.environ["THIS_EXISTS"] = "env_entry"
        assert test_config.env_var_entry == "env_entry"

    def test_empty_env_var_raises(self, test_config):
        os.environ.pop("THIS_EXISTS_NOT", None)
        with pytest.raises(KeyError) as e:
            test_config.empty_env_var_entry
            assert "THIS_EXISTS_NOT" in str(e)
