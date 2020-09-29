from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from config import get_config, DatabaseConfig
from orm.tables import Base


# TODO: set up develop database and test these methods in ci


def get_engine(db_config: DatabaseConfig = None):
    if db_config is None:
        c = get_config()
        db_config = c.database
    return create_engine(
        f"postgresql://{db_config.user}:{db_config.pw}@{db_config.host}:{db_config.port}/{db_config.name}",
        echo=db_config.log_statements,
    )


def get_session(db_config: DatabaseConfig = None) -> Session:
    engine = get_engine(db_config=db_config)
    return sessionmaker(bind=engine)()


# inspired by
# https://docs.sqlalchemy.org/en/13/orm/session_basics.html#when-do-i-construct-a-session-when-do-i-commit-it-and-when-do-i-close-it
@contextmanager
def session_scope(db_config: DatabaseConfig = None):
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
def engine_scope(session):
    """Provide a transactional scope around a database engine"""
    engine = session.get_bind()
    yield engine
    engine.dispose()


def get_table_model(table_name: str, db_config: DatabaseConfig = None):
    with session_scope(db_config) as session:
        session: Session
        table = Base.metadata.tables.get(table_name)
        if table is None:
            raise RuntimeError(
                f"No such table in database {session.bind.url.database}: {table_name}"
            )
    return table
