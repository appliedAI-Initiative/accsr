import os
import pytest

top_level_directory = os.path.dirname(__file__)

TEST_RESOURCES = os.path.join(top_level_directory , "resources")


@pytest.fixture()
def test_resources():
    return TEST_RESOURCES
