import requests
from dependencies.configuration import Configuration
from dependencies.constants import BatchRequestStatus
from dependencies.logger import logger
from dependencies.managers.database_manager import DatabaseManager

from models.batch_request import IEBatchRequestLog
from utility.aws import AwsUtility


class ExternalAPIHandler:

    def __init__(self):
        self.db_manager = DatabaseManager()
        self.db_session = self.db_manager.get_db(Configuration.BATCH_DB_CONNECTION_URL, Configuration.IE_DB)

    def process_completed_batches(self):
        """
        For batches in COMPLING_OUTPUT, call Sofi API and update the output S3 path
        """
        try:
            logger.info("Starting Sofi API processing for completed batches")

            completed_batches = self.db_session.query(IEBatchRequestLog).filter(
                IEBatchRequestLog.status == BatchRequestStatus.COMPLING_OUTPUT.value
            ).all()

            logger.info(f"Found {len(completed_batches)} batches in COMPLING_OUTPUT state")

            for batch in completed_batches:
                logger.info(f"Calling Sofi API for batch: {batch.id}")
                s3_url_output_key = batch.input_s3_url.replace('input', 'output').replace('.csv', '.xlsx')

                payload = {
                    "request_id": batch.request_id,
                    "input_file_path": batch.input_s3_url,
                    "input_file_url" : AwsUtility.create_presigned_url(batch.input_s3_url),
                    "output_file_path" : s3_url_output_key,
                    "output_file_url": AwsUtility.create_presigned_url(s3_url_output_key)
                }
                response = requests.post(Configuration.SOFTI_API_URL, json=payload, timeout=30)

                if response.status_code == 200:
                    result = response.json()
                    output_s3_path = result.get("output_file_path")

                    if not output_s3_path:
                        logger.error(f"No output_file_path received for batch {batch.id}")
                        batch.status = BatchRequestStatus.COMPILATION_ERROR.value
                    else:
                        batch.output_s3_url = output_s3_path
                        batch.status = BatchRequestStatus.COMPLETED.value
                        logger.info(f"Batch {batch.id} marked COMPLETED with output path: {output_s3_path}")
                else:
                    logger.error(f"Sofi API failed for batch {batch.id} with status code: {response.status_code}")
                    batch.status = BatchRequestStatus.COMPILATION_ERROR.value

                self.db_session.commit()


        except Exception:
            logger.exception("Error while processing Sofi API batches")
            self.db_session.rollback()
        finally:
            self.db_session.close()
            self.db_manager.dispose()
