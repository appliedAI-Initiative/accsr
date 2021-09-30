import json
import os
from urllib.parse import urljoin

import pytest
import requests
from libcloud.storage.providers import get_driver
from libcloud.storage.types import (
    ContainerAlreadyExistsError,
    InvalidContainerNameError,
)
from pytest_docker.plugin import get_docker_services
from requests.exceptions import ConnectionError

from accsr.config import ConfigProviderBase, DefaultDataConfiguration
from accsr.remote_storage import RemoteStorage, RemoteStorageConfig

top_level_directory = os.path.dirname(__file__)

TEST_RESOURCES = os.path.join(top_level_directory, "resources")


class __Configuration(DefaultDataConfiguration):
    @property
    def remote_storage(self):
        return RemoteStorageConfig(**self._get_non_empty_entry("remote_storage_config"))


class ConfigProvider(ConfigProviderBase[__Configuration]):
    pass


_config_provider = ConfigProvider()


def get_config() -> __Configuration:
    """
    :return: the configuration instance
    """
    config = _config_provider.get_config(
        config_files=["config_test.json", "config_local.json"]
    )
    return config


@pytest.fixture(scope="session")
def test_resources():
    return TEST_RESOURCES


@pytest.fixture(scope="session")
def running_on_ci() -> bool:
    return os.getenv("GITLAB_CI") is not None or os.getenv("CI") is not None


@pytest.fixture(scope="session")
def docker_services(
    docker_compose_file, docker_compose_project_name, docker_cleanup, running_on_ci
):
    """This overwrites pytest-docker's docker_services fixture to avoid starting containers on CI"""
    if running_on_ci:
        yield
    else:
        with get_docker_services(
            docker_compose_file, docker_compose_project_name, docker_cleanup
        ) as docker_service:
            yield docker_service


@pytest.fixture(scope="session")
def docker_compose_file(pytestconfig):
    return os.path.join(
        str(pytestconfig.rootdir), "tests", "accsr", "docker-compose.yaml"
    )


@pytest.fixture(scope="module")
def remote_storage_server(running_on_ci, docker_ip, docker_services):
    """Starts minio container and makes sure it is reachable.
    The containers will not be created on CI."""
    # Skips starting the container if we running on Gitlab CI or Github Actions
    if running_on_ci:
        return
    # `port_for` takes a container port and returns the corresponding host port
    port = docker_services.port_for("remote-storage", 9000)
    url = "http://{}:{}".format(docker_ip, port)

    def is_minio_responsive(url):
        url = urljoin(url, "minio/health/live")
        try:
            response = requests.get(url)
            if response.status_code == 200:
                return True
        except ConnectionError:
            return False

    docker_services.wait_until_responsive(
        timeout=30.0, pause=0.5, check=lambda: is_minio_responsive(url)
    )


@pytest.fixture(scope="module")
def remote_storage_config(running_on_ci):
    """This create a config_local.json on CI with remote_storage host set to 'remote-storage'.
    This value can be found in the docker-compose.yaml file."""
    if running_on_ci:
        with open(os.path.join(top_level_directory, "config_local.json"), "w") as f:
            json.dump(
                {"remote_storage_config": {"host": "remote-storage", "port": "9000"}}, f
            )


@pytest.fixture(scope="module")
def create_bucket(remote_storage_config, remote_storage_server):
    # create bucket if it doesn't exist already
    config = get_config().remote_storage
    storage_driver_factory = get_driver(config.provider)
    driver = storage_driver_factory(
        config.key, config.secret, host=config.host, port=config.port, secure=False
    )
    try:
        driver.create_container(container_name=config.bucket)
    except (ContainerAlreadyExistsError, InvalidContainerNameError):
        pass


@pytest.fixture()
def storage(remote_storage_server, create_bucket):
    storage = RemoteStorage(get_config().remote_storage)
    # This has to be set here unless we want to set up certificates for this
    # TODO: determine whether we should add this to possible_driver_kwargs or not?
    storage.driver_kwargs["secure"] = False
    return storage
