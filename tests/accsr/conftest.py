import os
from typing import Tuple
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

from accsr.remote_storage import RemoteStorage, RemoteStorageConfig

top_level_directory = os.path.dirname(__file__)

TEST_RESOURCES = os.path.join(top_level_directory, "resources")


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


@pytest.fixture(scope="session")
def remote_storage_server(running_on_ci, docker_ip, docker_services) -> Tuple[str, int]:
    """Starts minio container and makes sure it is reachable.
    The containers will not be created on CI."""
    # Skips starting the container if we running on Gitlab CI or Github Actions
    if running_on_ci:
        return "remote-storage", 9000
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
    return "localhost", port


@pytest.fixture(scope="session")
def remote_storage_config(running_on_ci, remote_storage_server) -> RemoteStorageConfig:
    config = RemoteStorageConfig(
        key="minio",
        secret="minio123",
        bucket="accsr-integration-tests",
        base_path="",
        provider="s3",
    )
    if running_on_ci:
        config.host = "remote-storage"
        config.port = "9000"
    else:
        config.host = remote_storage_server[0]
        config.port = remote_storage_server[1]
    return config


@pytest.fixture(scope="module")
def create_bucket(remote_storage_config, remote_storage_server):
    # create bucket if it doesn't exist already
    storage_driver_factory = get_driver(remote_storage_config.provider)
    driver = storage_driver_factory(
        key=remote_storage_config.key,
        secret=remote_storage_config.secret,
        host=remote_storage_config.host,
        port=remote_storage_config.port,
        secure=False,
    )
    try:
        driver.create_container(container_name=remote_storage_config.bucket)
    except (ContainerAlreadyExistsError, InvalidContainerNameError):
        pass


@pytest.fixture()
def storage(remote_storage_config, remote_storage_server, create_bucket):
    storage = RemoteStorage(remote_storage_config)
    # This has to be set here unless we want to set up certificates for this
    # TODO: determine whether we should add this to possible_driver_kwargs or not?
    storage.driver_kwargs["secure"] = False
    return storage
