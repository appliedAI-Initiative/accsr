import logging
import os
from typing import Generator

import pytest


@pytest.fixture(scope="module", autouse=True)
def change_to_resources_dir(test_resources, request):
    os.chdir(test_resources)
    yield
    os.chdir(request.config.invocation_dir)


@pytest.fixture()
def test_filename(
    change_to_resources_dir, storage, request
) -> Generator[str, None, None]:
    """Pushes a file to remote storage, yields its filename and then deletes it from remote storage"""
    filename = request.param
    storage.push_file(filename)
    yield filename
    storage.delete(filename)


NAME_COLLISIONS_DIR_NAME = "storage_name_collisions"


@pytest.fixture()
def setup_name_collision(change_to_resources_dir, storage):
    """
    Pushes files and dirs with colliding names to remote storage, yields files pushed
    and deletes everything at cleanup
    """
    pushed_objects = storage.push(NAME_COLLISIONS_DIR_NAME)
    yield pushed_objects
    storage.delete(NAME_COLLISIONS_DIR_NAME)


@pytest.fixture()
def test_dirname(
    change_to_resources_dir, storage, request
) -> Generator[str, None, None]:
    """Pushes a directory to remote storage, yields its name and then deletes it from remote storage"""
    dirname = request.param
    storage.push_directory(dirname)
    yield dirname
    storage.delete(dirname)


def test_delete_no_matches(storage, caplog):
    with caplog.at_level(logging.WARNING):
        deleted_files = storage.delete("there is no such file")
    assert len(deleted_files) == 0
    assert "Not deleting anything" in caplog.text


def test_delete_file(storage):
    storage.push_file("sample.txt", overwrite_existing=True)
    assert len(storage.list_objects("sample.txt")) == 1
    deleted_objects = storage.delete("sample.txt")
    assert len(deleted_objects) == 1
    assert len(storage.list_objects("sample.txt")) == 0


def test_delete_with_base_path(storage):
    base_path = "base_path"
    storage.set_remote_base_path(base_path)
    storage.push_file("sample.txt", overwrite_existing=True)
    assert len(storage.list_objects("sample.txt")) == 1
    deleted_objects = storage.delete("sample.txt")
    assert len(deleted_objects) == 1
    assert deleted_objects[0].name == f"{base_path}/sample.txt"


def test_delete_dir(storage):
    storage.push_directory("sample_dir", overwrite_existing=True)
    assert len(storage.list_objects("sample_dir")) == 2
    deleted_objects = storage.delete("sample_dir")
    assert len(deleted_objects) == 2
    assert len(storage.list_objects("sample_dir")) == 0


@pytest.mark.parametrize(
    "test_filename",
    ["sample.txt"],
    indirect=["test_filename"],
)
def test_push_file_empty_base_path(storage, test_filename):
    remote_objects = storage.push(test_filename)
    assert len(remote_objects) == 1
    # we need lstrip because s3 paths (and names) start with "/" while google storage paths start without it...
    assert remote_objects[0].name.lstrip("/") == test_filename


@pytest.mark.parametrize(
    "test_filename",
    ["sample.txt"],
    indirect=["test_filename"],
)
def test_push_file_nonempty_base_path(storage, test_filename):
    base_path = "base_path"
    storage.set_remote_base_path(base_path)
    remote_objects = storage.push(test_filename)
    assert len(remote_objects) == 1
    assert remote_objects[0].name.lstrip("/") == f"{base_path}/{test_filename}"


@pytest.mark.parametrize(
    "test_dirname",
    ["sample_dir"],
    indirect=["test_dirname"],
)
def test_push_directory(storage, test_dirname):
    remote_objects = storage.push(test_dirname)
    assert len(remote_objects) == 2
    assert len(storage.list_objects(test_dirname)) == 2


@pytest.mark.parametrize(
    "file_or_dir_name", ["non_existing_file.txt", "non_existing_dir"]
)
def test_push_non_existing(storage, file_or_dir_name):
    with pytest.raises(
        FileNotFoundError, match="does not refer to a file or directory"
    ):
        storage.push(file_or_dir_name)


@pytest.mark.parametrize(
    "test_filename",
    ["sample.txt"],
    indirect=["test_filename"],
)
def test_pull_file(storage, test_filename, tmpdir):
    local_base_dir = tmpdir.mkdir("remote_storage")
    storage.pull(test_filename, local_base_dir=local_base_dir)
    assert os.path.isfile(os.path.join(local_base_dir, test_filename))
    pulled_files = storage.pull(test_filename)
    assert len(pulled_files) == 0


@pytest.mark.parametrize(
    "test_filename",
    ["sample.txt"],
    indirect=["test_filename"],
)
def test_pull_file_to_existing_dir_path(storage, test_filename, tmpdir):
    local_base_dir = tmpdir.mkdir("remote_storage")
    local_base_dir.mkdir(test_filename)
    with pytest.raises(
        FileExistsError,
        match="Cannot pull file to a path which is an existing directory:",
    ):
        storage.pull(test_filename, local_base_dir=local_base_dir)


@pytest.mark.parametrize(
    "test_dirname",
    ["sample_dir"],
    indirect=["test_dirname"],
)
def test_pull_dir(storage, test_dirname, tmpdir):
    local_base_dir = tmpdir.mkdir("remote_storage")
    storage.pull(test_dirname, local_base_dir=local_base_dir)
    assert os.path.isdir(os.path.join(local_base_dir, test_dirname))
    assert len(os.listdir(os.path.join(local_base_dir, test_dirname))) == 2
    pulled_files = storage.pull(test_dirname)
    assert len(pulled_files) == 0


@pytest.mark.parametrize(
    "file_or_dir_name", ["non_existing_file.txt", "non_existing_dir"]
)
def test_pull_non_existing(storage, file_or_dir_name, caplog):
    with caplog.at_level(logging.WARNING):
        pulled_files = storage.pull(file_or_dir_name)
    assert len(pulled_files) == 0
    assert "No such remote file or directory" in caplog.text


def test_name_collisions_pulling_properly(setup_name_collision, storage, tmpdir):
    storage.set_remote_base_path(NAME_COLLISIONS_DIR_NAME)
    local_base_dir = tmpdir.mkdir("remote_storage")
    colliding_file_name = "file.txt.collision"
    colliding_dir_name = "dir_name_collision"

    storage.pull("file.txt", local_base_dir=local_base_dir)
    storage.pull("dir_name", local_base_dir=local_base_dir)
    assert not os.path.isfile(os.path.join(local_base_dir, colliding_file_name))
    assert os.path.isfile(os.path.join(local_base_dir, "file.txt"))

    assert not os.path.isdir(os.path.join(local_base_dir, colliding_dir_name))
    assert os.path.isdir(os.path.join(local_base_dir, "dir_name"))

    storage.pull(colliding_file_name, local_base_dir=local_base_dir)
    assert os.path.isfile(os.path.join(local_base_dir, colliding_file_name))

    storage.pull(colliding_dir_name, local_base_dir=local_base_dir)
    assert os.path.isfile(os.path.join(local_base_dir, colliding_dir_name, "file.txt"))


def test_name_collisions_deleting_properly(setup_name_collision, storage):
    storage.set_remote_base_path(NAME_COLLISIONS_DIR_NAME)
    storage.delete("file.txt")
    remaining_object_names = [
        obj.name.lstrip("/").lstrip(f"{NAME_COLLISIONS_DIR_NAME}/")
        for obj in storage.list_objects("")
    ]
    assert "file.txt" not in remaining_object_names
    assert "file.txt.collision" in remaining_object_names
    assert "dir_name/file.txt" in remaining_object_names


# TODO or not TODO: many cases are missing - checking names, testing overwriting.
