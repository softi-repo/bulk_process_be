from dependencies.logger import logger
from handlers.cron.cron_handler import failed_retry_cron, check_status_cron, batch_loader_cron


CRON_EVENT_FUNCTION_MAP = {
    "failed_retry_cron": failed_retry_cron,
    "check_status_cron": check_status_cron,
    "batch_loader_cron": batch_loader_cron
}


def handler(event=None, context=None):
    """
    Cron handler to handle CloudWatch triggered cron events.

    :param event: dict
    :param context: CloudWatchContext
    """
    cron = event.get('cron')
    logger.info(f"[DEBUG] Received event: {event}")
    if cron not in CRON_EVENT_FUNCTION_MAP.keys():
        logger.error(f'No Cron Function found: {cron}')
        return

    try:
        cron_function = CRON_EVENT_FUNCTION_MAP[cron]
        logger.info(f'Initiated Execution for cron function: {cron_function}')
        cron_function()

        logger.info(f'Completed Execution for cron function: {cron_function}')

    except Exception:
        logger.exception("Error occurred at cron.handler")
