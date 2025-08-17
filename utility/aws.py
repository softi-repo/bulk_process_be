import boto3

from botocore.exceptions import ClientError

from dependencies.configuration import Configuration
from dependencies.logger import logger


class AwsUtility:

    @staticmethod
    def create_presigned_url(
            s3_upload_key: str,
            mode: str = 'get_object',
            expiry_time: int = 86400
         ):

        """Generate a presigned URL to share an S3 object
        :param expiry_time:
        :param mode:
        :param s3_upload_key: str
        :return: str
        """
        logger.info("Inside create_presigned_url function")
        s3_client = boto3.client(
            's3',
            region_name=Configuration.AWS_REGION_NAME
            )

        logger.info(f'The s3_upload key is {s3_upload_key}')
        try:
            url = s3_client.generate_presigned_url(
                f'{mode}',
                Params={
                    'Bucket': Configuration.AWS_BUCKET,
                    'Key': s3_upload_key,
                },
                ExpiresIn=expiry_time
            )

            logger.info(f'The pre-singed url is {url}')
        except (ClientError, Exception) as e:
            logger.exception(f'Exception raised when uploading to S3 to generate the s3 url {e}')
            return
        return url

