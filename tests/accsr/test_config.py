from accsr.remote_storage import RemoteStorageConfig
from accsr.sql_access import DatabaseConfig


def test_db_config_repr_does_not_include_pw():
    """
    Ensure that str representation of database config does not leak password.

    Regression test for issue #6.
    """
    cfg = DatabaseConfig(
        "host", "name", "user", "1234", pw="secretpass", log_statements=False
    )

    assert cfg.pw not in repr(cfg)
    assert cfg.pw not in str(cfg)


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
