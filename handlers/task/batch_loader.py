import io
import json
import re
import time
from typing import Union, List

import pytz
import pandas as pd
import datetime

from starlette import status
from boto3.session import Session

from dependencies.configuration import Configuration
from dependencies.constants import BatchRequestStatus
from dependencies.logger import logger
from dependencies.managers.database_manager import DatabaseManager

from handlers.ecs_run_task_handler import ECSRunTaskHandler
from models.batch_request import IEBatchRequestLog
from models.batch_status import IeBatchRunLog

class BatchLoader:
    def __init__(self):
        self.db_manager = DatabaseManager()
        self.db_session = self.db_manager.get_db(Configuration.BATCH_DB_CONNECTION_URL, Configuration.IE_DB)

    def _process_pan_list_batch(self, batch_request_obj):
        """
        Process a batch where PANs are provided as a list in DB instead of a file.
        """
        ent_id = batch_request_obj.cid
        env = batch_request_obj.env
        logger.info("inside _process_pan_list_batch")
        # Assuming pan_list is stored as JSON string in DB
        try:
            pan_list = json.loads(batch_request_obj.pan_list)
            logger.info(f"pan list is {pan_list}")
        except Exception as e:
            logger.exception(f"Invalid PAN list for batch_request_id={batch_request_obj.id}: {e}")
            batch_request_obj.status = BatchRequestStatus.INVALID.value
            batch_request_obj.error_message = "INVALID PAN LIST"
            self.db_session.commit()
            return

        # Convert to DataFrame to reuse insert_into_batch_status_table logic
        df = pd.DataFrame({"pan": pan_list})
        self.db_session.commit()

        self.update_request_table(batch_request_obj.id)
        self.insert_into_batch_status_table(df, ent_id, batch_request_obj.id, env)

        logger.info("PAN list batch loading completed")
        ECSRunTaskHandler().create_ecs_task(ecs_task_name="check_status_task", ecs_task_params=())

    @staticmethod
    def sanitize_and_validate_pan(pan_raw: str) -> str | None:

        if not pan_raw:
            return None

        # Remove any non-alphanumeric characters
        pan_clean = re.sub(r"[^A-Za-z0-9]", "", pan_raw).upper()

        if len(pan_clean) != 10:
            return None

        if not re.match(r"^[A-Z]{5}[0-9]{4}[A-Z]$", pan_clean):
            return None

        return pan_clean

    @staticmethod
    def download_s3(input_s3_link: str, batch_request_obj: IEBatchRequestLog):
        """
        Download the s3_file from AWS S3 Bucket
        :param input_s3_link: s3_link from batch request table
        :param batch_request_obj : BatchRequest
        :return: df
        """
        logger.info("Inside Downloading S3")
        try:
            link = input_s3_link.split("/")
            session = Session()

            s3 = session.resource("s3")
            s3_file = s3.Object(bucket_name=Configuration.AWS_BUCKET, key="/".join(link[3:])).get()

            file_extension = batch_request_obj.input_s3_url.split(".")[-1].lower()
            if file_extension == "csv":
                df = pd.read_csv(io.BytesIO(s3_file["Body"].read()), dtype=str, encoding="utf-8")
            else:
                df = pd.read_excel(io.BytesIO(s3_file["Body"].read()), engine="openpyxl", dtype=str)

            logger.info("S3 File Downloaded Successfully")
            return df

        except Exception as e:
            logger.exception(f"Error occurred while downloading s3_file{e}")
            raise InterruptedError(f"{status.HTTP_400_BAD_REQUEST}|S3 Utility Failed")

    @staticmethod
    def __create_client_ref_id(ent_id: int, row_id: int, batch_request_auto_id: int):
        """
        Creates client reference number
        :param ent_id: ent id
        :param row_id: row id in Excel
        :param batch_request_auto_id: BatchRequest ID
        :return: client_ref_id
        """
        _date = datetime.date.today()

        return f"{ent_id}_{_date}_{batch_request_auto_id}_{row_id}"

    def insert_into_batch_status_table(
            self, df, ent_id: int, batch_request_auto_id: int, env
    ):
        chunk_size = 500
        logger.info(f"Batch Loader Chunk Size: {chunk_size}")
        df = df.fillna("")
        index = 0
        for start in range(0, df.shape[0], chunk_size):
            df_subset = df.iloc[start: start + chunk_size]
            batch_status_objs = []

            for idx, row in df_subset.iterrows():
                pan_raw = row.get("pan", "")

                pan = self.sanitize_and_validate_pan(pan_raw)
                existing_client_ref_id = row.get("client_ref_id")
                client_ref_id = existing_client_ref_id or self.__create_client_ref_id(
                    ent_id,
                    idx,
                    batch_request_auto_id
                )
                tz = pytz.timezone("Asia/Kolkata")

                request_body = json.dumps({
                            "client_ref_id": client_ref_id,
                             "pan": pan
                })

                batch_status_obj = IeBatchRunLog(
                    env= env,
                    cid=ent_id,
                    batch_request_auto_id=batch_request_auto_id,
                    client_ref_id=client_ref_id,
                    processing_status=BatchRequestStatus.OPEN.value,
                    batch_ref_num=index,
                    pan=pan,
                    request_body=request_body,
                    retry_count=0,
                    created_on=datetime.datetime.now(tz),
                    updated_on=datetime.datetime.now(tz),
                )

                batch_status_objs.append(batch_status_obj)
                index += 1

            self.__insert_to_batch_status(batch_status_objs)

    def __insert_to_batch_status(self, batch_status_objs: List):
        for _ in range(3):
            try:
                self.db_session.bulk_save_objects(batch_status_objs)
                self.db_session.commit()
                logger.info("Inserted into batch status table")
                return
            except Exception:
                logger.exception(
                    f"Error occurred while bulk saving [Start {batch_status_objs[0]}, End {batch_status_objs[-1]}]"
                )
                self.db_session.rollback()
                time.sleep(5)

    def update_request_table(self, batch_request_auto_id: int):
        """
        Updates the batch request status to IN_PROGRESS

        :param batch_request_auto_id: id of batch request row
        :returns: None
        """
        try:
            self.db_session.query(IEBatchRequestLog).filter_by(id=batch_request_auto_id).update(
                {"status": BatchRequestStatus.IN_PROGRESS.value}
            )

            self.db_session.commit()
            logger.info("Batch request status updated to IN_PROGRESS")

        except Exception:
            logger.exception("Error occurred while updating request table")

    def pending_batch_loader(self, received_request_id: Union[str, List[str]]):
        """
        1. Downloads Pending Excel file(s) from S3 Bucket
        2. Inserts details from Excel to batch status table
        3. Updates the batch request status to IN_PROGRESS
        """
        logger.info("Inside Pending Batch Loader")

        logger.info(f"Received request_id : {received_request_id}")

        batch_request_objs = (
            self.db_session.query(IEBatchRequestLog)
            .filter(
                IEBatchRequestLog.status == BatchRequestStatus.PENDING.value,
                IEBatchRequestLog.request_id == received_request_id
            )
            .all()
        )

        if not batch_request_objs:
            logger.info("No pending batch found")
            return

        for batch_request_obj in batch_request_objs:
            try:
                logger.info(f"Processing Batch Request ID: {batch_request_obj.id}")
                if batch_request_obj.pan_list:
                    self._process_pan_list_batch(batch_request_obj)
                else:
                    self._process_single_batch(batch_request_obj)
            except Exception:
                logger.exception(f"Exception occurred while processing Batch Request ID: {batch_request_obj.id}")
                batch_request_obj.status = BatchRequestStatus.INVALID.value
                batch_request_obj.error_message = "UPLOADED FILE IS INVALID"
                self.db_session.commit()

        self.db_session.close()
        self.db_manager.dispose()

    def _process_single_batch(self, batch_request_obj):
        logger.info("Inside the _process_single_batch")
        ent_id = batch_request_obj.cid

        input_s3_link = batch_request_obj.input_s3_url
        logger.info(f"S3_LINK: {input_s3_link}")
        df = self.download_s3(input_s3_link, batch_request_obj)
        df.columns = df.columns.str.strip().str.lower()
        env = batch_request_obj.env
        self.db_session.commit()

        self.update_request_table(batch_request_obj.id)
        self.insert_into_batch_status_table(df, ent_id, batch_request_obj.id, env)

        logger.info("Batch Loading Completed")

        ECSRunTaskHandler().create_ecs_task(ecs_task_name="check_status_task", ecs_task_params=())
