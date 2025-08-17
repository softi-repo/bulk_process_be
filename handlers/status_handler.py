from starlette import status

from dependencies.configuration import Configuration
from dependencies.constants import BatchRequestStatus
from dependencies.logger import logger

from models.batch_request import IEBatchRequestLog

from utility.aws import AwsUtility


class StatusHandler:

    def __init__(self, db_session):
        self.db_session = db_session

    def get_batch_request_status(self, request_id: str):
        """
        Display the current statics along with the request_id to front end when request_id is provided in from client
        """
        logger.info('Inside status_api function')

        logger.info(f'The request_id from the input api: {request_id}')

        batch_request_obj = self.db_session.query(IEBatchRequestLog).filter(IEBatchRequestLog.request_id == request_id).first()

        if not batch_request_obj:
            logger.error(f"No batch found for request_id: {request_id}")
            raise InterruptedError(f"{status.HTTP_404_NOT_FOUND}|No batch found for request_id: {request_id}")

        dict_for_status_api = {}

        dict_for_status_api.update({
            "http_response_code": status.HTTP_200_OK,
            "client_ref_id": batch_request_obj.client_ref_id,
            "request_id": batch_request_obj.request_id,
            "result":
                {
                    "current_statistics": batch_request_obj.current_statistics
                },
            "status": batch_request_obj.status
        })

        s3_url_key = batch_request_obj.input_s3_url.replace('input', 'output').replace('.csv', '.xlsx')
        s3_url_key = s3_url_key.replace(f's3://{Configuration.AWS_BUCKET}/', '')

        if batch_request_obj.status == BatchRequestStatus.COMPLETED.value:
            dict_for_status_api.update({
                "download_pre_singed_url": AwsUtility.create_presigned_url(s3_url_key)
            })
        logger.info(f'The current_statics  {dict_for_status_api}')

        return dict_for_status_api
