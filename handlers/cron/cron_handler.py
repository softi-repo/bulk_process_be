from datetime import datetime
from dependencies.logger import logger


from handlers.cron.batch_loader_cron import BatchScheduler
from handlers.cron.failed_retry import FailedRetry
from handlers.task.check_status import CheckStatus


def failed_retry_cron():
    """Triggers the failed retry cron."""
    logger.info(f'Triggering failed_retry task at {datetime.now()}')
    try:
        FailedRetry().failed_retry_cron()
    except Exception as e:
        logger.exception(f'some exception occurred in failed_retry {e}')
    finally:
        logger.info(f'Completing failed_retry task at {datetime.now()}')


def check_status_cron():
    """Triggers the check status cron."""
    logger.info(f'Triggering check_status task at {datetime.now()}')
    try:
        CheckStatus().update_current_statistics()
    except Exception as e:
        logger.exception(f'some exception occurred in check_status {e}')
    finally:
        logger.info(f'Completing check_status task at {datetime.now()}')

def batch_loader_cron():
    """Triggers the scheduled batches."""
    logger.info(f'Triggering batch_loader_cron task at {datetime.now()}')
    try:
        BatchScheduler().check_and_load()
    except Exception as e:
        logger.exception(f'some exception occurred in batch_loader_cron {e}')
    finally:
        logger.info(f'Completing batch_loader_cron task at {datetime.now()}')
