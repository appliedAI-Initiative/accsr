from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from config import get_config, DatabaseConfig


def get_engine(db_config: DatabaseConfig = None):
    if db_config is None:
        c = get_config()
        db_config = c.database
    return create_engine(
        f"postgresql://{db_config.user}:{db_config.pw}@{db_config.host}:{db_config.port}/{db_config.name}"
    )


def get_session(db_config: DatabaseConfig = None) -> Session:
    engine = get_engine(db_config=db_config)
    return sessionmaker(bind=engine)()


# inspired by
# https://docs.sqlalchemy.org/en/13/orm/session_basics.html#when-do-i-construct-a-session-when-do-i-commit-it-and-when-do-i-close-it
@contextmanager
def session_scope(db_config: DatabaseConfig = None):
    """Provide a transactional scope around a series of operations."""
    session = get_session(db_config)
    try:
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()
