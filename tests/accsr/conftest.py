import os
import sys

import pytest

from accsr.remote_storage import RemoteStorage

sys.path.append(os.path.abspath("."))
from config import get_config

top_level_directory = os.path.dirname(__file__)

TEST_RESOURCES = os.path.join(top_level_directory, "resources")


@pytest.fixture()
def test_resources():
    return TEST_RESOURCES


@pytest.fixture()
def storage():
    return RemoteStorage(get_config().remote_storage)
