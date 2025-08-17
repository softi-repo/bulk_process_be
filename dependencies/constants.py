from enum import Enum
from http import HTTPStatus


class BaseEnum(Enum):

    @classmethod
    def has_value(cls, value):
        return value in cls._value2member_map_


class BatchRequestStatus(BaseEnum):

    PENDING = 'PENDING'
    IN_PROGRESS = 'Inprogress'
    COMPLETED = 'Completed'
    COMPLING_OUTPUT = 'Compling_output'
    FAILURE = 'Failure'
    ERROR = 'Error'
    INVALID = 'INVALID'
    COMPILING = 'COMPILING'
    COMPILATION_ERROR = 'COMPILATION_ERROR'
    NOT_PURGED = 'NOT_PURGED'
    PURGE_COMPLETED = 'PURGE_COMPLETED'
    OPEN = 'Open'
    DUPLICATE = 'DUPLICATE'

class Constants:
    DEFAULT_RESPONSE_HEADERS = {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Headers': 'Origin, Content-Type, X-Auth-Token, Authorization',
        'Access-Control-Allow-Credentials': 'true',
        'X-Frame-Options': 'DENY',
        'X-XSS-Protection': '1; mode=block',
        'X-Content-Type-Options': 'nosniff',
        'Referrer-Policy': 'strict-origin-when-cross-origin',
        'Cache-Control': 'must-revalidate',
        'Strict-Transport-Security': 'max-age=31536000; includeSubDomains; preload',
        'Connection': 'Keep-Alive',
        'Content-Security-Policy': "default-src 'self'"
    }


    METHOD_NOT_ALLOWED_JSON = {
            'http_response_code': str(HTTPStatus.METHOD_NOT_ALLOWED.value),
            'message': 'Invalid Method'
        }


ERROR_MAPPING_CONSTANT = {
    "MISSING_EXCEL_SIZE_LIMIT": {
        "message": "Missing 'EXCEL_SIZE_LIMIT' configuration."
    },
    "MISSING_SERVICE_CONFIG": {
        "message": "Missing configuration for the given service."
    },
    "EXCEEDS_BATCH_SIZE_LIMIT": {
        "message": "Batch size exceeds allowed limit."
    },
    "MISSING_INPUT_CONFIG": {
        "message": "No input configuration found for the given input_type."
    },
    "MISSING_REQUIRED_COLUMNS": {
        "message": "Input file is missing one or more required columns."
    },
    "DUPLICATE_ROWS": {
        "message": "Duplicated records found."
    },
    "EXCEPTION": {
        "message": "Exception during batch file validation."
    }
}


FAILURE_RETRY_LIMIT = 3
INTERNAL_SERVER_ERROR = "Internal Server Error"
CLIENT_REF_REGEX = "^[a-zA-Z0-9_-]{1,100}$"
