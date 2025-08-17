import json
from datetime import datetime, timedelta

from starlette import status

from dependencies.configuration import Configuration
from dependencies.constants import BatchRequestStatus, BatchConfigureConstant
from dependencies.logger import logger

from handlers.download_handler import DownloadHandler

from models.batch_request import BatchRequest
from models.bpe_batch_config import BatchConfig


class HistoryHandler:

    def __init__(self, db_session):
        self.db_session = db_session


    def history_api(self, ent_id: int, page_number: int, rows: int, service_id: str, start_date: str, end_date: str, env: str):

        logger.info('Inside history_api function')
        logger.info(f'The ent_id from the input api: {ent_id}')

        start_date_obj = datetime.strptime(start_date, '%Y-%m-%d') if start_date else None
        end_date_obj = datetime.strptime(end_date, '%Y-%m-%d') + timedelta(days=1) - timedelta(seconds=1) if end_date else None

        query_params = [ BatchRequest.ent_id == ent_id, BatchRequest.parent_id == None ]

        if start_date_obj and end_date_obj:
            query_params.append(BatchRequest.created_on >= start_date_obj)
            query_params.append(BatchRequest.created_on <= end_date_obj)

        if service_id:
            query_params.append(BatchRequest.service_id == int(service_id))

        batch_request_objs = self.db_session.query(BatchRequest).filter(*query_params).order_by(
            BatchRequest.id.desc()).limit(rows).offset(
            page_number * rows
        ).all()

        batch_request_objs_count = self.db_session.query(BatchRequest).filter(*query_params).count()

        batch_request_compiled_list = []
        dict_of_history_handler = {
            "http_response_code": status.HTTP_200_OK,
            "total_number_of_batches": len(batch_request_objs),
            "total_record": batch_request_objs_count
        }

        for batch_request_obj in batch_request_objs:

            if batch_request_obj.request_id and batch_request_obj.current_statistics:
                batch_request_compiled_list.append({
                    "client_ref_num": batch_request_obj.client_ref_num,
                    "request_id": batch_request_obj.request_id,
                    "current_statistics": batch_request_obj.current_statistics,
                    "service_id": batch_request_obj.service_id,
                    # "download_url": True if batch_request_obj.output_s3_url else False,
                    "download_url": (
                        DownloadHandler().download_excel_sheet(batch_request_obj.ent_id, batch_request_obj.request_id).get("result", {}).get("pre_singed_url", None)
                        if DownloadHandler().download_excel_sheet(batch_request_obj.ent_id, batch_request_obj.request_id) else None
                    ),
                    "batch_request_status": batch_request_obj.status,
                    "created_on": batch_request_obj.created_on,
                    "updated_on": batch_request_obj.updated_on,
                    "purge_status": batch_request_obj.purge_status,
                    "scheduled_on": batch_request_obj.scheduled_on,
                    "parent_id": batch_request_obj.parent_id,
                    "is_parent": batch_request_obj.is_parent,
                    "error_message": batch_request_obj.error_message
                })
            else:
                batch_request_compiled_list.append({
                    "client_ref_num": batch_request_obj.client_ref_num,
                    "request_id": batch_request_obj.request_id,
                    "current_statistics": None,
                    "batch_request_status": batch_request_obj.status,
                    "service_id": batch_request_obj.service_id,
                    "created_on": batch_request_obj.created_on,
                    "purge_status": batch_request_obj.purge_status,
                    "scheduled_on": batch_request_obj.scheduled_on,
                    "parent_id": batch_request_obj.parent_id,
                    "is_parent": batch_request_obj.is_parent,
                    "error_message": batch_request_obj.error_message,
                    "updated_on": batch_request_obj.updated_on

                })
            dict_of_history_handler.update({
                "result": batch_request_compiled_list,
            })

        logger.info(f'The batch request_request id list {batch_request_compiled_list}')
        return dict_of_history_handler