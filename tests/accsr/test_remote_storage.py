import logging
import os
from pathlib import Path
from typing import Generator

import pytest


@pytest.fixture(scope="module", autouse=True)
def change_to_resources_dir(test_resources, request):
    os.chdir(test_resources)
    yield
    os.chdir(request.config.invocation_dir)


@pytest.fixture()
def file_on_storage(
    change_to_resources_dir, storage, request
) -> Generator[str, None, None]:
    """Pushes a file to remote storage, yields its filename and then deletes it from remote storage"""
    filename = request.param
    storage.push(filename)
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
def dir_on_storage(
    change_to_resources_dir, storage, request
) -> Generator[str, None, None]:
    """Pushes a directory to remote storage, yields its name and then deletes it from remote storage"""
    dirname = request.param
    storage.push(dirname)
    yield dirname
    storage.delete(dirname)


class TestRemoteStorage:
    def test_delete_no_matches(self, storage, caplog):
        with caplog.at_level(logging.WARNING):
            deleted_files = storage.delete("there is no such file")
        assert len(deleted_files) == 0
        assert "Not deleting anything" in caplog.text

    def test_delete_file(self, storage):
        storage.push("sample.txt", force=True)
        assert len(storage.list_objects("sample.txt")) == 1
        deleted_objects = storage.delete("sample.txt")
        assert len(deleted_objects) == 1
        assert len(storage.list_objects("sample.txt")) == 0

    def test_push_regex_and_glob(self, storage, change_to_resources_dir):
        # excludes sample_2.txt as well as sample_dir/*
        storage.push(
            "*", force=True, include_regex="sample.*txt", exclude_regex="sample_.*"
        )
        assert len(storage.list_objects("sample")) == 1
        storage.delete("sample.txt")

    def test_pull_regex(self, storage, change_to_resources_dir):
        storage.push(
            "*", force=True, include_regex="sample.*txt", exclude_regex="sample_dir.*"
        )
        assert len(storage.list_objects("sample")) == 2
        summary = storage.pull(
            "", include_regex="sample.*txt", exclude_regex="sample_2.*"
        )
        assert len(summary.matched_source_files) == 1
        summary = storage.pull("", include_regex="sample.*txt")
        assert len(summary.matched_source_files) == 2
        storage.delete("", include_regex="sample.*txt")

    def test_delete_regex(self, storage, change_to_resources_dir):
        storage.push(
            "*", force=True, include_regex="sample.*txt", exclude_regex="sample_dir.*"
        )
        assert len(storage.list_objects("sample")) == 2
        deleted_objects = storage.delete(
            "", include_regex="sample.*txt", exclude_regex="sample_2.*"
        )
        assert len(deleted_objects) == 1
        assert len(storage.list_objects("sample_2.txt")) == 1
        deleted_objects = storage.delete("", include_regex="sample.*txt")
        assert len(deleted_objects) == 1

    def test_delete_with_base_path(self, storage):
        base_path = "base_path"
        storage.set_remote_base_path(base_path)
        storage.push("sample.txt", force=True)
        assert len(storage.list_objects("sample.txt")) == 1
        deleted_objects = storage.delete("sample.txt")
        assert len(deleted_objects) == 1
        assert deleted_objects[0].name == f"{base_path}/sample.txt"

    def test_delete_dir(self, storage):
        storage.push("sample_dir", force=True)
        assert len(storage.list_objects("sample_dir")) == 2
        deleted_objects = storage.delete("sample_dir")
        assert len(deleted_objects) == 2
        assert len(storage.list_objects("sample_dir")) == 0

    def test_push_file_empty_base_path(self, storage, change_to_resources_dir):
        test_filename = "sample.txt"
        push_summary = storage.push(test_filename)
        assert len(push_summary.synced_files) == 1
        # we need lstrip because s3 paths (and names) start with "/" while google storage paths start without it...
        assert push_summary.synced_files[0].name.lstrip("/") == test_filename
        storage.delete(test_filename)

    def test_push_file_nonempty_base_path(self, storage):
        base_path = "base_path"
        test_filename = "sample.txt"
        storage.set_remote_base_path(base_path)
        push_summary = storage.push(test_filename)
        assert len(push_summary.synced_files) == 1
        assert (
            push_summary.synced_files[0].name.lstrip("/")
            == f"{base_path}/{test_filename}"
        )
        storage.delete(test_filename)

    def test_push_file_local_path_prefix(self, storage, test_resources):
        assert len(storage.list_objects("sample.txt")) == 0
        test_filename = "sample.txt"
        push_summary = storage.push(test_filename, local_path_prefix=test_resources)
        assert len(push_summary.synced_files) == 1
        # Now same file with absolute path, should not need to push again
        push_summary = storage.push(
            Path(test_resources) / test_filename, local_path_prefix=test_resources
        )
        assert len(push_summary.synced_files) == 0
        storage.delete(test_filename)

    def test_push_file_local_path_prefix_and_glob(self, storage, test_resources):
        test_filename = "s*le_2.txt"  # matches only sample_2.txt
        assert len(storage.list_objects("sample_2.txt")) == 0
        push_summary = storage.push(test_filename, local_path_prefix=test_resources)
        assert len(push_summary.synced_files) == 1
        # Now same file with absolute path, should not need to push again
        push_summary = storage.push(
            Path(test_resources) / test_filename, local_path_prefix=test_resources
        )
        assert len(push_summary.synced_files) == 0
        storage.delete("sample_2.txt")

    def test_push_directory(self, storage):
        assert len(storage.list_objects("sample_dir")) == 0
        test_dirname = "sample_dir"
        push_summary = storage.push(test_dirname)
        assert len(push_summary.synced_files) == 2
        assert len(storage.list_objects(test_dirname)) == 2
        storage.delete(test_dirname)

    @pytest.mark.parametrize(
        "file_or_dir_name", ["non_existing_file.txt", "non_existing_dir"]
    )
    def test_push_non_existing(self, storage, file_or_dir_name):
        with pytest.raises(FileNotFoundError, match=file_or_dir_name):
            storage.push(file_or_dir_name)

    @pytest.mark.parametrize(
        "file_on_storage",
        ["sample.txt"],
        indirect=["file_on_storage"],
    )
    def test_pull_file(self, storage, file_on_storage, tmpdir):
        local_base_dir = tmpdir.mkdir("remote_storage")
        storage.pull(file_on_storage, local_base_dir=local_base_dir)
        assert os.path.isfile(os.path.join(local_base_dir, file_on_storage))
        pull_summary = storage.pull(file_on_storage, force=False)
        assert len(pull_summary.synced_files) == 0

    @pytest.mark.parametrize(
        "file_on_storage",
        ["sample.txt"],
        indirect=["file_on_storage"],
    )
    def test_push_existing_file(self, storage, file_on_storage):
        assert len(storage.list_objects(file_on_storage)) == 1
        push_summary = storage.push(file_on_storage, force=False)
        assert len(push_summary.synced_files) == 0
        push_summary = storage.push(file_on_storage, force=True)
        # still zero because we are pushing the same file
        assert len(push_summary.synced_files) == 0

    @pytest.mark.parametrize(
        "file_on_storage",
        ["sample.txt"],
        indirect=["file_on_storage"],
    )
    def test_pull_file_to_existing_dir_path(self, storage, file_on_storage, tmpdir):
        local_base_dir = tmpdir.mkdir("remote_storage")
        local_base_dir.mkdir(file_on_storage)
        with pytest.raises(
            FileExistsError,
            match=r".*directory:.*",
        ):
            storage.pull(file_on_storage, local_base_dir=local_base_dir)

    @pytest.mark.parametrize(
        "dir_on_storage",
        ["sample_dir"],
        indirect=["dir_on_storage"],
    )
    def test_pull_dir(self, storage, dir_on_storage, tmpdir):
        local_base_dir = tmpdir.mkdir("remote_storage")
        storage.pull(dir_on_storage, local_base_dir=local_base_dir)
        assert os.path.isdir(os.path.join(local_base_dir, dir_on_storage))
        assert len(os.listdir(os.path.join(local_base_dir, dir_on_storage))) == 2
        pull_summary = storage.pull(dir_on_storage, force=False)
        assert len(pull_summary.synced_files) == 0

    @pytest.mark.parametrize(
        "file_or_dir_name", ["non_existing_file.txt", "non_existing_dir"]
    )
    def test_pull_non_existing(self, storage, file_or_dir_name, caplog):
        with caplog.at_level(logging.WARNING):
            pull_summary = storage.pull(file_or_dir_name)
        assert len(pull_summary.synced_files) == 0
        assert "No files found in remote storage under path:" in caplog.text

    def test_name_collisions_pulling_properly(
        self, setup_name_collision, storage, tmpdir
    ):
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
        assert os.path.isfile(
            os.path.join(local_base_dir, colliding_dir_name, "file.txt")
        )

    def test_name_collisions_deleting_properly(self, setup_name_collision, storage):
        storage.set_remote_base_path(NAME_COLLISIONS_DIR_NAME)
        storage.delete("file.txt")
        remaining_object_names = [
            obj.name.lstrip("/").lstrip(f"{NAME_COLLISIONS_DIR_NAME}/")
            for obj in storage.list_objects("")
        ]
        assert "file.txt" not in remaining_object_names
        assert "file.txt.collision" in remaining_object_names
        assert "dir_name/file.txt" in remaining_object_names

    def test_summary_repr(self, storage, change_to_resources_dir):
        summary = storage.push("*")
        assert summary.sync_direction == "push"
        summary.print_short_summary()
        assert isinstance(summary.to_json(), str)
        assert isinstance(repr(summary), str)

    # TODO: several cases are still missing - e.g. testing overwriting with force=True.
