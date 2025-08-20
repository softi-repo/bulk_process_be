from starlette import status

from dependencies.logger import logger

from models.batch_request import IEBatchRequestLog


class HistoryHandler:

    def __init__(self, db_session):
        self.db_session = db_session

    def history_api(self, ent_id: int, page_number: int, no_of_rows: int, env: str):

        logger.info('Inside history_api function')
        logger.info(f'The ent_id from the input api: {ent_id}')

        query_params = [IEBatchRequestLog.ent_id == ent_id, IEBatchRequestLog.env == env ]

        batch_request_objs = self.db_session.query(IEBatchRequestLog).filter(*query_params).order_by(
            IEBatchRequestLog.id.desc()).limit(no_of_rows).offset(
            page_number * no_of_rows
        ).all()

        batch_request_objs_count = self.db_session.query(IEBatchRequestLog).filter(*query_params).count()

        batch_request_compiled_list = []
        dict_of_history_handler = {
            "http_response_code": status.HTTP_200_OK,
            "total_number_of_batches": len(batch_request_objs),
            "total_record": batch_request_objs_count
        }

        for batch_request_obj in batch_request_objs:

            if batch_request_obj.request_id and batch_request_obj.current_statistics:
                batch_request_compiled_list.append({
                    "client_ref_id": batch_request_obj.client_ref_id,
                    "request_id": batch_request_obj.request_id,
                    "current_statistics": batch_request_obj.current_statistics,
                    "batch_request_status": batch_request_obj.status,
                    "created_on": batch_request_obj.created_on,
                    "updated_on": batch_request_obj.updated_on,
                    "error_message": batch_request_obj.error_message
                })
            else:
                batch_request_compiled_list.append({
                    "client_ref_id": batch_request_obj.client_ref_id,
                    "request_id": batch_request_obj.request_id,
                    "current_statistics": None,
                    "batch_request_status": batch_request_obj.status,
                    "created_on": batch_request_obj.created_on,
                    "error_message": batch_request_obj.error_message,
                    "updated_on": batch_request_obj.updated_on
                })
            dict_of_history_handler.update({
                "result": batch_request_compiled_list,
            })

        logger.info(f'The batch request_request id list {batch_request_compiled_list}')
        return dict_of_history_handler