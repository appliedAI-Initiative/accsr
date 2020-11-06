from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import ContextManager

try:
    from sqlalchemy import create_engine
    from sqlalchemy.engine import Engine
    from sqlalchemy.orm import Session, sessionmaker
except ImportError:
    raise ImportError(
        "Trying to import sql_access module but SQLAlchemy is not installed. "
        "Install with pip install data_access[sql]"
    )


@dataclass
class DatabaseConfig:
    host: str
    name: str
    user: str
    port: str
    pw: str = field(repr=False)
    log_statements: bool = False


def get_engine(db_config: DatabaseConfig) -> Engine:
    return create_engine(
        f"postgresql://{db_config.user}:{db_config.pw}@{db_config.host}:{db_config.port}/{db_config.name}",
        echo=db_config.log_statements,
    )


def get_session(db_config: DatabaseConfig) -> Session:
    engine = get_engine(db_config)
    return sessionmaker(bind=engine)()


# inspired by
# https://docs.sqlalchemy.org/en/13/orm/session_basics.html#when-do-i-construct-a-session-when-do-i-commit-it-and-when-do-i-close-it
@contextmanager
def session_scope(db_config: DatabaseConfig) -> ContextManager[Session]:
    """Provide a transactional scope around a series of operations"""
    session = get_session(db_config)
    try:
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()


@contextmanager
def engine_scope(session) -> ContextManager[Engine]:
    """Provide a transactional scope around a database engine"""
    engine = session.get_bind()
    yield engine
    engine.dispose()
