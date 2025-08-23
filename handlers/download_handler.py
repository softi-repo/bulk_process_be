from starlette import status

from dependencies.configuration import Configuration
from dependencies.constants import BatchRequestStatus
from dependencies.logger import logger
from dependencies.managers.database_manager import DatabaseManager
from models.batch_request import  IEBatchRequestLog

from utility.aws import AwsUtility


class DownloadHandler:

    def __init__(self):
        self.db_manager = DatabaseManager()
        self.db_session = self.db_manager.get_db(Configuration.BATCH_DB_CONNECTION_URL, Configuration.IE_DB)

    def download_excel_sheet(self, ent_id: int, request_id: str, env: str):
        """
        Display the current statics,request_id,and download link for s3

        :param ent_id: int
        :param request_id: string
        :param env: str
        :return :dict_of_download
        """
        try:
            logger.info('Inside download_api Function')

            logger.info(f'The request body from the client is {request_id}')
            batch_request_obj = self.db_session.query(IEBatchRequestLog).filter(
                IEBatchRequestLog.request_id == request_id,
                IEBatchRequestLog.ent_id == ent_id,
                IEBatchRequestLog.env == env
            ).first()

            if not batch_request_obj:
                logger.error('No Batch found for the supplied Authentication Token.')
                raise InterruptedError(f'{status.HTTP_404_NOT_FOUND}|No IEBatchRequestLog Found')

            logger.info(f'The request_id from the database table is {batch_request_obj.request_id}')
            logger.info(f'The id of the batch_request_obj:  {batch_request_obj.id}')
            dict_of_download = {}

            s3_url_key = batch_request_obj.input_s3_url.replace('input', 'output').replace('.csv', '.xlsx')
            s3_url_key = s3_url_key.replace(f's3://{Configuration.AWS_BUCKET}/', '')

            if batch_request_obj.output_s3_url:
                dict_of_download.update({
                    "http_response_code": status.HTTP_200_OK,
                    "client_ref_number": batch_request_obj.client_ref_num,
                    "request_id": batch_request_obj.request_id,
                    "result": {
                        "current_statistics": batch_request_obj.current_statistics,
                        "s3_url": batch_request_obj.output_s3_url,
                        "pre_singed_url": AwsUtility.create_presigned_url(s3_url_key)
                    }})
                return dict_of_download

            if batch_request_obj.status != BatchRequestStatus.COMPLETED.value:

                return {
                    "http_response_code": status.HTTP_102_PROCESSING,
                    "client_ref_number": batch_request_obj.client_ref_num,
                    "request_id": batch_request_obj.request_id,
                    "message": "Batch is under process"
                      }

        except InterruptedError as e:
            raise

        except Exception as e:
            logger.exception(f"Exception occurred in download_excel_sheet {e}")
        finally:
            self.db_session.close()
            self.db_manager.dispose()