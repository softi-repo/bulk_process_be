import json
import re

import boto3
from starlette import status

from dependencies.configuration import Configuration
from dependencies.logger import logger

from handlers.smtp_handler import SMTPHandler


class ECSRunTaskHandler:

    @staticmethod
    def __get_latest_task_definition(ecs_client: boto3.client, ecs_task_family: str):
        version = re.search(r':\d+$', ecs_task_family)
        if version:
            ecs_task_family = ecs_task_family[: -1 * (len(version.group()))]

        logger.info(f"Getting Latest Task Definition For {ecs_task_family}")
        response = ecs_client.list_task_definitions(
            familyPrefix=ecs_task_family,
            sort='DESC',
            maxResults=1
        )

        if not response.get('taskDefinitionArn'):
            logger.error(f"No task definitions found for family '{ecs_task_family}'")
            return ecs_task_family

        latest_task_definition = response.get('taskDefinitionArn')
        logger.info(f"Latest Task Definition Found: {latest_task_definition}")
        return latest_task_definition

    def create_ecs_task(
        self,
        ecs_task_name: str,
        ecs_task_params: tuple
    ):
        """
        Create ECS
        :param ecs_task_name: ECS task name
        :param ecs_task_params: ECS task params
        :return:
        """
        logger.info(f'Creating ECS task for {ecs_task_name}.')

        try:
            client = boto3.client('ecs')

            task_def = Configuration.ECS_TASK_DEFINITION

            response = client.run_task(
                cluster=Configuration.ECS_CLUSTER,
                launchType=Configuration.FARGATE,
                taskDefinition=self.__get_latest_task_definition(
                    client,
                    task_def
                ),
                count=1,
                overrides={
                    'containerOverrides': [
                        {
                            'name': Configuration.ECS_CONTAINER_NAME,
                            'command': ['python3', 'tasks.py', ecs_task_name, json.dumps(ecs_task_params)]
                        }
                    ],
                    'taskRoleArn': Configuration.TASK_ROLE_ARN
                },
                platformVersion='LATEST',
                networkConfiguration={
                    'awsvpcConfiguration': {
                        'subnets': Configuration.SUBNETS,
                        'assignPublicIp': 'ENABLED',
                        'securityGroups': Configuration.SECURITY_GROUP,
                    }
                }
            )

            logger.info(f'ECS Task Trigger Response: {response}')
            if response.get('failures'):
                logger.error('Error occurred while creating ECS task.')
                SMTPHandler().send_aws_ses_exception(
                    error_message=f'Error occurred while creating ECS task: {response["failures"]}'
                )
                raise InterruptedError(f'{status.HTTP_422_UNPROCESSABLE_ENTITY}|ECS_TASK_CREATION_FAILED')

        except Exception as e:
            logger.exception('Exception occurred while creating ECS task')
            SMTPHandler().send_aws_ses_exception(
                error_message=f'Exception occurred while creating ECS task: {e}'
            )