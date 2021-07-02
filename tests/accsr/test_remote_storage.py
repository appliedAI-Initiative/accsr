import logging
import os

import pytest


@pytest.fixture()
def change_to_resources_dir(test_resources, request):
    os.chdir(test_resources)
    yield
    os.chdir(request.config.invocation_dir)


def test_delete_no_mathes(storage, caplog):
    with caplog.at_level(logging.WARNING):
        deleted_files = storage.delete("there is no such file")
    assert deleted_files == []
    assert "Not deleting anything" in caplog.text


def test_delete_file(storage, change_to_resources_dir):
    storage.push_file("sample.txt", overwrite_existing=True)
    assert len(storage.list_objects("sample.txt")) == 1
    deleted_objects = storage.delete("sample.txt")
    assert len(deleted_objects) == 1
    assert len(storage.list_objects("sample.txt")) == 0


def test_delete_with_base_path(storage, change_to_resources_dir):
    base_path = "base_path"
    storage.set_remote_base_path(base_path)
    storage.push_file("sample.txt", overwrite_existing=True)
    assert len(storage.list_objects("sample.txt")) == 1
    deleted_objects = storage.delete("sample.txt")
    assert len(deleted_objects) == 1
    assert deleted_objects[0].name == f"{base_path}/sample.txt"


def test_delete_dir(storage, change_to_resources_dir):
    storage.push_directory("sample_dir", overwrite_existing=True)
    assert len(storage.list_objects("sample_dir")) == 2
    deleted_objects = storage.delete("sample_dir")
    assert len(deleted_objects) == 2
    assert len(storage.list_objects("sample_dir")) == 0


# TODO: improve setup and cleanup for tests involving pushing


def test_push_file_empty_base_path(storage, change_to_resources_dir):
    storage.delete("sample.txt")
    remote_objects = storage.push("sample.txt")
    assert len(remote_objects) == 1
    # we need lstrip because s3 paths (and names) start with "/" while google storage paths start without it...
    assert remote_objects[0].name.lstrip("/") == "sample.txt"
    storage.delete("sample.txt")


def test_push_file_nonempty_base_path(storage, change_to_resources_dir):
    base_path = "base_path"
    storage.set_remote_base_path(base_path)
    storage.delete("sample.txt")
    remote_objects = storage.push("sample.txt")
    assert len(remote_objects) == 1
    assert remote_objects[0].name.lstrip("/") == f"{base_path}/sample.txt"
    storage.delete("sample.txt")


def test_push_directory(storage, change_to_resources_dir):
    storage.delete("sample_dir")
    remote_objects = storage.push("sample_dir")
    assert len(remote_objects) == 2
    assert len(storage.list_objects("sample_dir")) == 2
    storage.delete("sample_dir")


def test_pull_file(storage, change_to_resources_dir, tmpdir):
    local_base_dir = tmpdir.mkdir("remote_storage")
    storage.push("sample.txt")
    storage.pull("sample.txt", local_base_dir=local_base_dir)
    assert os.path.isfile(os.path.join(local_base_dir, "sample.txt"))
    pulled_files = storage.pull("sample.txt")
    assert pulled_files == []
    storage.delete("sample.txt")


def test_pull_dir(storage, change_to_resources_dir, tmpdir):
    local_base_dir = tmpdir.mkdir("remote_storage")
    storage.push("sample_dir")
    storage.pull("sample_dir", local_base_dir=local_base_dir)
    assert os.path.isdir(os.path.join(local_base_dir, "sample_dir"))
    assert len(os.listdir(os.path.join(local_base_dir, "sample_dir"))) == 2
    pulled_files = storage.pull("sample_dir")
    assert pulled_files == []
    storage.delete("sample_dir")


# TODO or not TODO: many cases are missing - pulling/pushing nonexisting files, checking names, testing overwriting.
