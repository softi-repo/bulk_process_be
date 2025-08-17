import threading

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from dependencies.configuration import Configuration
from dependencies.logger import logger


class DatabaseManager:

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DatabaseManager, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        self.thread_pool = threading.local()

    def get_db(self, db_url, schema):
        connection_pool = getattr(self.thread_pool, "connection_pool", {})
        connection_string = db_url + schema
        try:
            if connection_pool.get(connection_string):
                logger.info(f"DB Connection already present for {connection_string}")
                return Session(connection_pool[connection_string], future=True)
            else:
                logger.info(f"DB Connection Pool: {connection_pool}")
                return self.reset_db_conn(db_url, schema)
        except Exception as e:
            logger.exception(f"An exception has occurred in the Get DB function. {e}")

    def reset_db_conn(self, db_url, schema):
        logger.info(f"Resetting DB connection: {db_url}")
        engine = None
        connection_string = db_url + schema
        connection_pool = getattr(self.thread_pool, "connection_pool", {})
        try:
            if connection_pool.get(connection_string):
                connection_pool[connection_string].dispose()


        except Exception as e:
            logger.exception("An exception has occurred in the Reset DB connection.")

        _params = {
            "pool_recycle": 3600,
            "pool_size": Configuration.DB_POOL_SIZE,
            "max_overflow": Configuration.DB_MAX_OVERFLOW,
            "pool_timeout": 150,
        }

        for _ in range(3):
            try:
                engine = create_engine(connection_string, **_params)
                break
            except Exception as e:
                logger.exception(f"An exception has occurred in create Engine.{e}")
                engine = None
                continue

        if engine is not None:
            logger.info(f"Created...{id(engine)} - {engine.url.host} for {schema}")

            connection_pool[connection_string] = engine
            self.thread_pool.connection_pool = connection_pool

        return Session(engine, future=True)

    def dispose(self):
        logger.info("Inside DB Dispose function")
        connection_pool = getattr(self.thread_pool, "connection_pool", {})
        for connection_string in list(connection_pool.keys()):
            try:
                logger.info(f"Disposing connection: {connection_string}")
                connection_pool[connection_string].dispose()
            except Exception as e:
                logger.exception(f"Exception occurred - {e}")

            connection_pool.pop(connection_string, None)
            logger.info(f"Removed {connection_string} from the thread pool.")

        logger.info("Completed DB disposition.")
