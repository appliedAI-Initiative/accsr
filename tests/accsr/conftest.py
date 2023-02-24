import logging
import os
from typing import Tuple
from urllib.parse import urljoin

import pytest
import requests
from pytest_docker.plugin import Services, get_docker_services
from requests.exceptions import ConnectionError

from accsr.remote_storage import RemoteStorage, RemoteStorageConfig


@pytest.fixture(scope="session")
def test_resources():
    return os.path.join(os.path.dirname(__file__), "resources")


@pytest.fixture(scope="session")
def running_on_ci() -> bool:
    return os.getenv("GITLAB_CI") is not None or os.getenv("CI") is not None


@pytest.fixture(scope="session")
def docker_services(
    docker_compose_command,
    docker_compose_file,
    docker_compose_project_name,
    docker_setup,
    docker_cleanup,
    running_on_ci,
):
    """This overwrites pytest-docker's docker_services fixture to avoid starting containers on CI"""
    if running_on_ci:
        yield
    else:
        logging.info(
            f"Starting minio inside a docker container for remote storage tests"
        )
        with get_docker_services(
            docker_compose_command,
            docker_compose_file,
            docker_compose_project_name,
            docker_setup,
            docker_cleanup,
        ) as docker_service:
            yield docker_service


@pytest.fixture(scope="session")
def docker_compose_file(pytestconfig):
    return os.path.join(
        str(pytestconfig.rootdir), "tests", "accsr", "docker-compose.yaml"
    )


def port_for_windows_fix(
    docker_services: Services, service: str, container_port: int
) -> int:
    """This is a workaround for the port_for function not working on windows"""
    output = docker_services._docker_compose.execute(
        "port %s %d" % (service, container_port)
    )
    endpoint = output.strip().decode("utf-8")
    # This handles messy output that might contain warnings or other text
    endpoint_parts = endpoint.split("\r\n")
    if len(endpoint_parts) > 1:
        endpoint = endpoint_parts[0]
    # Usually, the IP address here is 0.0.0.0, so we don't use it.
    match = int(endpoint.split(":", 1)[1])
    return match


@pytest.fixture(scope="session")
def remote_storage_server(running_on_ci, docker_ip, docker_services) -> Tuple[str, int]:
    """Starts minio container and makes sure it is reachable.
    The containers will not be created on CI."""
    # Skips starting the container if running in CI
    if running_on_ci:
        return "remote-storage", 9000
    # `port_for` takes a container port and returns the corresponding host port
    if os.name == "nt":
        # port_for doesn't work on windows
        port = port_for_windows_fix(docker_services, "remote-storage", 9000)
    else:
        port = docker_services.port_for("remote-storage", 9000)
    url = f"http://{docker_ip}:{port}"

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
    if running_on_ci:
        host = "remote-storage"
        port = "9000"
    else:
        host = remote_storage_server[0]
        port = remote_storage_server[1]
    config = RemoteStorageConfig(
        provider="s3",
        key="minio-root-user",
        secret="minio-root-password",
        bucket="accsr-integration-tests",
        base_path="",
        host=host,
        port=port,
        secure=False,
    )
    return config


@pytest.fixture()
def storage(remote_storage_config, remote_storage_server):
    storage = RemoteStorage(remote_storage_config)
    storage.create_bucket()
    return storage
