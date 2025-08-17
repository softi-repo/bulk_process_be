from dependencies.configuration import Configuration
from dependencies.constants import BatchRequestStatus
from dependencies.logger import logger
from dependencies.managers.database_manager import DatabaseManager
from handlers.output_api_handler import ExternalAPIHandler

from models.batch_request import IEBatchRequestLog
from models.batch_status import IeBatchRunLog


class CheckStatus:

    def __init__(self):
        self.db_manager = DatabaseManager()
        self.db_session = self.db_manager.get_db(Configuration.BATCH_DB_CONNECTION_URL, Configuration.IE_DB)

    def update_current_statistics(self):
        """
        Query for the update of the current_static_batch
        """
        try:
            logger.info("Inside the update_current_statistics function")
            batch_request_objs = self.db_session.query(IEBatchRequestLog).filter(
                IEBatchRequestLog.status == BatchRequestStatus.IN_PROGRESS.value
            ).all()

            logger.info(f'Count of Pending Batch Request: {len(batch_request_objs)}')

            for batch_request_obj in batch_request_objs:
                logger.info(f'Creating current_statistics for BatchRequest: {batch_request_obj.id}')

                batch_request_obj.current_statistics = {
                    'failure': self.db_session.query(IeBatchRunLog).filter_by(
                        batch_request_auto_id=batch_request_obj.id,
                        processing_status=BatchRequestStatus.FAILURE.value
                    ).count(),
                    'completed': self.db_session.query(IeBatchRunLog).filter_by(
                        batch_request_auto_id=batch_request_obj.id,
                        processing_status=BatchRequestStatus.COMPLETED.value
                    ).count(),
                    'open': self.db_session.query(IeBatchRunLog).filter_by(
                        batch_request_auto_id=batch_request_obj.id,
                        processing_status=BatchRequestStatus.OPEN.value
                    ).count(),
                    'total': self.db_session.query(IeBatchRunLog).filter_by(
                        batch_request_auto_id=batch_request_obj.id
                    ).count(),
                    'error': self.db_session.query(IeBatchRunLog).filter_by(
                        batch_request_auto_id=batch_request_obj.id,
                        processing_status=BatchRequestStatus.ERROR.value
                    ).count(),
                    'inprogress': self.db_session.query(IeBatchRunLog).filter_by(
                        batch_request_auto_id=batch_request_obj.id,
                        processing_status=BatchRequestStatus.IN_PROGRESS.value
                    ).count()
                }

                if (
                    (
                        batch_request_obj.current_statistics.get('failure', 0) +
                        batch_request_obj.current_statistics.get('completed', 0)
                    ) == batch_request_obj.current_statistics.get('total', 0) and
                    batch_request_obj.current_statistics.get('total', 0) != 0
                ):
                    batch_request_obj.status = BatchRequestStatus.COMPLING_OUTPUT.value

                    self.db_session.commit()
                    ExternalAPIHandler().process_completed_batches()

                self.db_session.commit()

                logger.info(
                    f'Statistics Generated for BatchRequest [{batch_request_obj.id}]: {batch_request_obj.current_statistics}'
                )

        except Exception:
            logger.exception('Some exception occurred in updating the current_statistics column')
            self.db_session.rollback()
        finally:
            self.db_session.close()
            self.db_manager.dispose()
