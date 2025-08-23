from typing import Any, Dict
from starlette import status

from dependencies.logger import logger
from models.batch_request import IEBatchRequestLog


class MultipleStatusHandler:
    def __init__(self, db_session):
        self.db_session = db_session

    def multiple_status_api(self, ent_id: int, env: str, request_id: str) -> Dict[str, Any]:
        logger.info("Inside multiple_status_api function")
        logger.info(f"Input params → ent_id: {ent_id}, env: {env}, request_id: {request_id}")

        batch_request_obj = (
            self.db_session.query(IEBatchRequestLog)
            .filter(
                IEBatchRequestLog.cid == ent_id,
                IEBatchRequestLog.env == env,
                IEBatchRequestLog.request_id == request_id,
            )
            .order_by(IEBatchRequestLog.id.desc())
            .first()
        )

        response: Dict[str, Any] = {"http_response_code": status.HTTP_200_OK}

        if batch_request_obj is not None:
            response["result"] = {
                "id": batch_request_obj.id,
                "client_ref_id": batch_request_obj.client_ref_id,
                "request_id": batch_request_obj.request_id,
                "current_statistics": batch_request_obj.current_statistics,
                "batch_request_status": batch_request_obj.status,
                "created_on": batch_request_obj.created_on.isoformat() if batch_request_obj.created_on else None,
                "updated_on": batch_request_obj.updated_on.isoformat() if batch_request_obj.updated_on else None,
                "number_of_pan": len(batch_request_obj.pan_list) if batch_request_obj.pan_list else 0,
                "error_message": batch_request_obj.error_message,
            }
        else:
            response["result"] = None

        logger.info(f"Batch request details → {response['result']}")
        return response
