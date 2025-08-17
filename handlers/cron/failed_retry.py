
from dependencies.constants import BatchRequestStatus
from dependencies.configuration import Configuration
from dependencies.managers.database_manager import DatabaseManager
from dependencies.logger import logger
from models.batch_request import IEBatchRequestLog
from models.batch_status import IeBatchRunLog

from sqlalchemy import and_

class FailedRetry:

    @staticmethod
    def failed_retry_cron():
        db_manager = DatabaseManager()
        db_session = db_manager.get_db(Configuration.BATCH_DB_CONNECTION_URL, Configuration.IE_DB)

        try:
            logger.info('[FAILED_RETRY] Starting failed_retry_cron job')

            # Get all batch requests that are currently IN_PROGRESS
            batch_requests = db_session.query(IEBatchRequestLog).filter(
                IEBatchRequestLog.status == BatchRequestStatus.IN_PROGRESS.value
            ).all()

            updates_to_commit = []

            for batch_request in batch_requests:
                logger.info(f"[FAILED_RETRY] Checking BatchRequest ID: {batch_request.id}")

                # Get all errored runs for this batch
                errored_runs = db_session.query(IeBatchRunLog).with_for_update().filter(
                    and_(
                        IeBatchRunLog.batch_request_auto_id == batch_request.id,
                        IeBatchRunLog.processing_status == BatchRequestStatus.ERROR.value
                    )
                ).all()

                for run in errored_runs:
                    if run.retry_count >= 3:
                        logger.info(f"[FAILED_RETRY] Marking BatchRun ID {run.id} as FAILURE (retry_count={run.retry_count})")
                        run.processing_status = BatchRequestStatus.FAILURE.value
                    else:
                        logger.info(f"[FAILED_RETRY] Retrying BatchRun ID {run.id} (retry_count={run.retry_count} → {run.retry_count + 1})")
                        run.retry_count += 1
                        run.processing_status = BatchRequestStatus.OPEN.value

                    updates_to_commit.append(run)

            if updates_to_commit:
                db_session.bulk_save_objects(updates_to_commit)
                db_session.commit()
                logger.info(f"[FAILED_RETRY] Updated {len(updates_to_commit)} BatchRun entries.")

        except Exception as e:
            logger.exception("[FAILED_RETRY] Exception occurred in failed_retry_cron")
            db_session.rollback()
        finally:
            if db_session:
                db_session.close()
            if db_manager:
                db_manager.dispose()
