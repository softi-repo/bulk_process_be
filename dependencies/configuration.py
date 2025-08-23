import os

from pathlib import Path

from os.path import dirname

from dotenv import load_dotenv


BASEDIR = Path(dirname(dirname(__file__)))


class Configuration:

    ENV = os.getenv('ENV','STAGE')
    path = f"{BASEDIR}/env/{ENV.lower()}.env"
    print(f"Loading from {path}")
    load_dotenv(dotenv_path=path)

    SOFTI_DB_CONNECTION_URL = os.getenv('SOFTI_DB_CONNECTION_URL')
    BATCH_DB_CONNECTION_URL = os.getenv('BATCH_DB_CONNECTION_URL')

    CS_DB = os.getenv('CS_DB')
    IE_DB = os.getenv('IE_DB', "ie")

    AWS_BUCKET = os.getenv('AWS_BUCKET')
    AWS_REGION_NAME = os.getenv('AWS_REGION_NAME')

    MAX_LENGTH_OF_PAN_LIST = os.getenv('MAX_LENGTH_OF_PAN_LIST')

    TASK_ROLE_ARN = os.getenv('TASK_ROLE_ARN')

    SOFTI_API_URL =os.getenv('SOFTI_API_URL')

    SENDER_EMAIL = 'alerts@digitap.ai'
    SENDER_NAME = 'Softi Exception'
    SENDER_NAME_FOR_CLIENT = 'Softi Batch'

    SMTP_USERNAME = os.getenv("SMTP_USERNAME")
    SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")

    SMTP_HOST = 'email-smtp.ap-south-1.amazonaws.com'
    SMTP_PORT = 587

    DB_POOL_SIZE = int(os.getenv("DB_POOL_SIZE", 10))
    DB_MAX_OVERFLOW = int(os.getenv("DB_MAX_OVERFLOW", 10))

    JWT_SECRET = os.getenv("JWT_SECRET")
    AES_SECRET_KEY = b'dXNlcl9tYW5hZ2Vt'

    # ECS configurations
    ECS_CLUSTER = os.getenv('ECS_CLUSTER')
    ECS_CONTAINER_NAME = os.getenv('ECS_CONTAINER_NAME')
    ECS_TASK_DEFINITION = os.getenv('ECS_TASK_DEFINITION')
    FARGATE = 'FARGATE'
    SECURITY_GROUP, SUBNETS = {
        'PROD': (
            ['sg-03831d298dbbaeeb4'],
            ['subnet-07aafc02dbf9bab5f', 'subnet-06f46baa1885de2a9']),
        'DEMO': (
            ['sg-0c0d3e2a5ed598fa6'],
            ['subnet-09d8046e73fc23634', 'subnet-02ad2bad080bcfecb', 'subnet-03cfacacb395e05f4']
        ),
        'STAGE': (
            ['sg-04f7c155fcf1478e4'],  # ✅ updated security group
            [  # ✅ updated subnets
                'subnet-09eea48c21de85af3',
                'subnet-0ec01df3f2723152b',
                'subnet-0956b6766145efd7e'
            ]
        )
    }[ENV]

    @staticmethod
    def init_config(env):
        DB_SCHEMA_ENV = {
            "Dev": {
                "CS_DB": "cs",
            },
            "Demo": {
                "CS_DB": "cs_demo",
            },
            "Prod": {
                "CS_DB": "cs",
            }
        }
        Configuration.CS_DB = DB_SCHEMA_ENV[env]['CS_DB']