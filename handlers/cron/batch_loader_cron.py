from dependencies.configuration import Configuration
from dependencies.constants import BatchRequestStatus
from dependencies.logger import logger
from dependencies.managers.database_manager import DatabaseManager
from handlers.ecs_run_task_handler import ECSRunTaskHandler
from models.batch_request import IEBatchRequestLog


class BatchScheduler:

    def __init__(self):
        self.db_manager = DatabaseManager()
        self.db_session = self.db_manager.get_db(Configuration.BATCH_DB_CONNECTION_URL, Configuration.IE_DB)

    def check_and_load(self):
        logger.info("Inside check_and_load")
        try:
            batch_request_objs = self.db_session.query(IEBatchRequestLog).filter(
                IEBatchRequestLog.status == BatchRequestStatus.PENDING.value
            ).all()

            for batch_request_obj in batch_request_objs:
                self.__load_batch(batch_request_obj)

        except Exception:
            logger.exception('The error occurred inside the check_and_load')
        finally:
            self.db_session.close()
            self.db_manager.dispose()

    @staticmethod
    def __load_batch( batch_request_obj: IEBatchRequestLog):

        ECSRunTaskHandler().create_ecs_task(
            ecs_task_name='batch_loader_task',
            ecs_task_params=(batch_request_obj.request_id,)
        )
