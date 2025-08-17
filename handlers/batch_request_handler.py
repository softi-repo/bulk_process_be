import datetime
import json
import os
import re
import tempfile

import boto3
import pandas as pd
import pytz

from botocore.exceptions import ClientError
from fastapi import UploadFile
from starlette import status

from dependencies.constants import CLIENT_REF_REGEX, BatchRequestStatus
from dependencies.configuration import Configuration
from dependencies.logger import logger

from models.batch_request import IEBatchRequestLog
from handlers.ecs_run_task_handler import ECSRunTaskHandler
from utility.common import CommonUtils


class BatchRequestHandler:

    def __init__(self, db_session):
        self.db_session = db_session

    def handle_batch_request(
        self,
        ent_id: int,
        client_ref_id: str,
        request_id: str,
        file_extension: str,
        file,
        env
    ):
        logger.info(f"Uploaded file: {file.filename}")

        if not all([client_ref_id, file]):
            logger.error(f"Missing the common thing : {client_ref_id}")
            raise InterruptedError(f"{status.HTTP_400_BAD_REQUEST}|Invalid client_ref_id format")

        if not client_ref_id or not re.match(CLIENT_REF_REGEX, client_ref_id):
            logger.error(f"Invalid client_ref_id: {client_ref_id}")
            raise InterruptedError(f"{status.HTTP_400_BAD_REQUEST}|Invalid client_ref_id format")

        tz = pytz.timezone("Asia/Kolkata")
        now = datetime.datetime.now(tz)

        file_extension = file_extension.lower() if file_extension else ""

        if file_extension == "csv":
            file_extension = "csv"
        else:
            file_extension = "xlsx"

        s3_key = f"{request_id}/input/{request_id}.{file_extension}"
        s3_bucket = Configuration.AWS_BUCKET

        required_columns = {"pan"}
        temp_file_path, length_of_df = self.process_and_validate_file(file, file_extension, required_columns)

        self.upload_file_to_s3(temp_file_path, s3_bucket, s3_key)

        batch_request_obj = IEBatchRequestLog(
            client_ref_id=client_ref_id,
            cid=ent_id,
            input_s3_url=f"s3://{s3_bucket}/{s3_key}",
            total_count=length_of_df,
            request_id=request_id,
            status=BatchRequestStatus.PENDING.value,
            env=env,
            created_on=now,
            updated_on=now
        )

        self.db_session.add(batch_request_obj)
        self.db_session.commit()

        logger.info(f"BatchRequest created for request_id={request_id}, cid={ent_id}")

        ECSRunTaskHandler().create_ecs_task(
            ecs_task_name="batch_loader_task",
            ecs_task_params=(request_id,)
        )

        response = {
            "http_response_code": status.HTTP_200_OK,
            "client_ref_id": client_ref_id,
            "request_id": request_id,
            "result": {
                "status": True,
                "message": "File validated, uploaded to S3, and batch created"
            }
        }

        logger.info(f"Batch request response: {json.dumps(response)}")
        return response

    @staticmethod
    def process_and_validate_file(file: UploadFile, file_extension: str, required_columns: set) -> (str, int):
        """
        Save the uploaded file to a temporary file, validate its structure, and return its file path.
        Raises InterruptedError if validation fails.
        """
        logger.info("Inside process_and_validate_file")
        try:
            # Save uploaded file to temp
            suffix = ".csv" if file_extension.lower() == "csv" else ".xlsx"
            with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                tmp.write(file.file.read())
                temp_file_path = tmp.name

            # Load DataFrame
            if file_extension.lower() == "csv":
                df = pd.read_csv(temp_file_path)
            elif file_extension.lower() == "xlsx":
                df = pd.read_excel(temp_file_path)
            else:
                raise InterruptedError(f"{status.HTTP_400_BAD_REQUEST}|Unsupported file extension: {file_extension}")

            # Check if file is empty
            if df.empty:
                raise InterruptedError(f"{status.HTTP_400_BAD_REQUEST}|File is empty")

            # Normalize column names
            df.columns = df.columns.str.strip().str.lower()
            required_columns_lower = {col.lower() for col in required_columns}

            # Check missing columns
            missing = required_columns_lower - set(df.columns)
            if missing:
                raise InterruptedError(f"{status.HTTP_400_BAD_REQUEST}|Missing columns: {', '.join(missing)}")

            # Duplicate check
            if df.duplicated(keep=False).any():
                raise InterruptedError(f"{status.HTTP_400_BAD_REQUEST}|Duplicate records found")

            # PAN sanitization
            df["sanitized_pan"] = df["pan"].apply(CommonUtils.sanitize_and_validate_pan)

            # Check for invalid PANs
            invalid_rows = df[df["sanitized_pan"].isnull()]
            if not invalid_rows.empty:
                invalid_list = ", ".join(invalid_rows["pan"].astype(str).unique())
                raise InterruptedError(f"{status.HTTP_400_BAD_REQUEST}|Invalid PAN(s) File")

            return temp_file_path, len(df)

        except InterruptedError:
            raise
        except Exception as e:
            logger.error(f"Error processing file: {e}", exc_info=True)
            raise InterruptedError(f"{status.HTTP_500_INTERNAL_SERVER_ERROR}|Error processing file")

    @staticmethod
    def upload_file_to_s3(file_path: str, s3_bucket: str, s3_key: str):
        """
        Upload a file to S3 using boto3's upload_file method.
        """
        logger.info("Inside the upload file to s3 function")
        s3 = boto3.resource('s3')

        try:
            s3.meta.client.upload_file(file_path, s3_bucket, s3_key)
            logger.info(f"Uploaded file to s3://{s3_bucket}/{s3_key}")
        except ClientError as e:
            logger.error(f"S3 upload failed: {str(e)}")
            raise InterruptedError(f"{status.HTTP_500_INTERNAL_SERVER_ERROR}|S3 upload failed")
        finally:
            if os.path.exists(file_path):
                os.unlink(file_path)  # Clean up

    def handle_batch_request_list_object(
            self,
            ent_id: int,
            client_ref_id: str,
            request_id: str,
            pan_list,
            env: str
    ):
        pan_list = json.loads(pan_list) if isinstance(pan_list, str) else pan_list
        print("pan_list", pan_list)
        if not all([client_ref_id, pan_list]):
            logger.error(f"Missing the common thing : {client_ref_id}")
            raise InterruptedError(f"{status.HTTP_400_BAD_REQUEST}|Invalid client_ref_id format")

        if not client_ref_id or not re.match(CLIENT_REF_REGEX, client_ref_id):
            logger.error(f"Invalid client_ref_id: {client_ref_id}")
            raise InterruptedError(f"{status.HTTP_400_BAD_REQUEST}|Invalid client_ref_id format")

        tz = pytz.timezone("Asia/Kolkata")
        now = datetime.datetime.now(tz)

        length_of_pan_list = len(pan_list)

        if length_of_pan_list >= int(Configuration.MAX_LENGTH_OF_PAN_LIST):
            raise InterruptedError(f"{status.HTTP_400_BAD_REQUEST}|Pan List Length exceed the limit")

        batch_request_obj = IEBatchRequestLog(
            client_ref_id=client_ref_id,
            cid=ent_id,
            pan_list=json.dumps(pan_list),
            total_count=length_of_pan_list,
            request_id=request_id,
            status=BatchRequestStatus.PENDING.value,
            env=env,
            created_on=now,
            updated_on=now
        )

        self.db_session.add(batch_request_obj)
        self.db_session.commit()

        logger.info(f"BatchRequest created for request_id={request_id}, cid={ent_id}")

        ECSRunTaskHandler().create_ecs_task(
            ecs_task_name="batch_loader_task",
            ecs_task_params=(request_id,)
        )

        response = {
            "http_response_code": status.HTTP_200_OK,
            "client_ref_id": client_ref_id,
            "request_id": request_id,
            "result": {
                "status": True,
                "message": "Pan List validated and batch created"
            }
        }

        logger.info(f"Batch request response: {json.dumps(response)}")
        return response
