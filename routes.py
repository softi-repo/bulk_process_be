import uuid
from typing import Optional

from fastapi import Request, Response, Form, APIRouter, UploadFile, File
from starlette import status

from dependencies.configuration import Configuration
from dependencies.authenticator import Authenticator
from dependencies.constants import INTERNAL_SERVER_ERROR
from dependencies.managers.database_manager import DatabaseManager
from dependencies.logger import logger

from handlers.batch_request_handler import BatchRequestHandler
from handlers.status_handler import StatusHandler
from utility.common import CommonUtils

api_router = APIRouter()



def get_db_sessions():
    db_manager = DatabaseManager()
    softi_session = db_manager.get_db(Configuration.SOFTI_DB_CONNECTION_URL, Configuration.CS_DB)
    batch_session = db_manager.get_db(Configuration.BATCH_DB_CONNECTION_URL, Configuration.IE_DB)
    return db_manager, softi_session, batch_session


def close_sessions(db_manager, softi_session, batch_session):
    softi_session.close()
    batch_session.close()
    db_manager.dispose()


def handle_error(e: Exception, request_id: str, response: Response):
    if isinstance(e, InterruptedError):
        status_code, error_message = str(e).split("|")
        response.status_code = int(status_code)
        return {
            "error": error_message,
            "status": False,
            "request_id": request_id
        }
    else:
        logger.exception("Unhandled server error")
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return {
            "http_response_code": response.status_code,
            "request_id": request_id,
            "error": INTERNAL_SERVER_ERROR
        }


@api_router.post("/v1/request")
async def batch_request(
    request: Request,
    response: Response,
    client_ref_id: str = Form(),
    file_extension: Optional[str] = Form(default=None),
    file: UploadFile = File(...)
):
    request_id = str(uuid.uuid4())
    logger.info(f"[REQUEST] Headers: {dict(request.headers)} | Client Ref: {client_ref_id} | Session ID: {request_id}")
    host = request.headers.get("host", "")
    common_util_obj = CommonUtils()
    env = common_util_obj.determine_environment(host)

    db_manager, softi_session, batch_session = get_db_sessions()
    try:
        ent_id, _ = Authenticator().validate(request.headers, softi_session, service_id = 43)
        response_body = BatchRequestHandler(batch_session).handle_batch_request(
            ent_id, client_ref_id, request_id, file_extension, file, env
        )
    except Exception as e:
        response_body = handle_error(e, request_id, response)
    finally:
        close_sessions(db_manager, softi_session, batch_session)

    logger.info(f"[REQUEST] Response: {response_body}")
    return response_body


@api_router.get("/v1/status/{request_id}")
async def batch_status(request_id: str, response: Response, request: Request):
    session_id = str(uuid.uuid4())
    logger.info(f"[STATUS] Headers: {dict(request.headers)} | Request ID: {request_id} | Session ID: {session_id}")

    db_manager, softi_session, batch_session = get_db_sessions()
    try:
        Authenticator().validate(request.headers, softi_session)
        response_body = StatusHandler(batch_session).get_batch_request_status(request_id)
    except Exception as e:
        response_body = handle_error(e, session_id, response)
    finally:
        close_sessions(db_manager, softi_session, batch_session)

    logger.info(f"[STATUS] Response: {response_body}")
    return response_body


@api_router.post("/v1/request-list")
async def batch_request(
    request: Request,
    response: Response,
    client_ref_id: str = Form(),
    pan_list: str = Form()
):
    request_id = str(uuid.uuid4())
    logger.info(f"[REQUEST] Headers: {dict(request.headers)} | Client Ref: {client_ref_id} | Session ID: {request_id}")
    host = request.headers.get("host", "")
    common_util_obj = CommonUtils()
    env = common_util_obj.determine_environment(host)

    db_manager, softi_session, batch_session = get_db_sessions()
    try:
        ent_id, _ = Authenticator().validate(request.headers, softi_session, service_id = 43)
        response_body = BatchRequestHandler(batch_session).handle_batch_request_list_object(
            ent_id, client_ref_id, request_id, pan_list, env
        )
    except Exception as e:
        response_body = handle_error(e, request_id, response)
    finally:
        close_sessions(db_manager, softi_session, batch_session)

    logger.info(f"[REQUEST] Response: {response_body}")
    return response_body