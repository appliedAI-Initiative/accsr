from accsr.remote_storage import RemoteStorageConfig


def test_storage_config_repr_does_not_include_secret():
    """
    Ensure that str representation of storage config does not leak secret.

    Regression test for issue #6.
    """
    cfg = RemoteStorageConfig(
        "provider", "key", "bucket", "secretkey", "region", "host", "1234", "base_path"
    )

    assert cfg.secret not in repr(cfg)
    assert cfg.secret not in str(cfg)
