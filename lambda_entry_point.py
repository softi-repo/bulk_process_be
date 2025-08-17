import json

import cron
from dependencies.logger import logger

from main import app
from mangum import Mangum

fastapi_handler = Mangum(app)

def lambda_handler(event, context):
    logger.info(f"[DEBUG] Incoming event: {json.dumps(event)}")

    # 1. CloudWatch / EventBridge CRON
    if event.get("source") == "aws.events":
        logger.info("[ENTRYPOINT] Detected AWS CRON event")
        return cron.handler(event, context)

    # 2. Custom cron test event
    if "cron" in event:
        logger.info(f"[ENTRYPOINT] Detected custom CRON event: {event['cron']}")
        return cron.handler(event, context)

    # 3. API Gateway / ALB event
    logger.info("[ENTRYPOINT] Detected backend/API event")
    return fastapi_handler(event, context)
