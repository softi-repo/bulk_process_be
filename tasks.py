import json
import sys
from datetime import datetime

from dependencies.logger import logger
from handlers.task.batch_loader import BatchLoader
from handlers.task.check_status import CheckStatus

"""
Standalone Task Executor (Celery-Free)
Compatible with ECS Run Task or manual CLI
"""


def batch_loader_task(request_ids=None):
    """Triggers the batch loader task."""
    logger.info(f'Triggering batch_loader task at {datetime.now()}')
    try:
        BatchLoader().pending_batch_loader(received_request_id=request_ids)
    except Exception as e:
        logger.exception(f'Some exception occurred in batch_loader: {e}')
    finally:
        logger.info(f'Completing batch_loader task at {datetime.now()}')


def check_status_task(_args=None):
    """Triggers the check status task."""
    logger.info(f'Triggering check_status task at {datetime.now()}')
    try:
        CheckStatus().update_current_statistics()
    except Exception as e:
        logger.exception(f'Some exception occurred in check_status: {e}')
    finally:
        logger.info(f'Completing check_status task at {datetime.now()}')


"""
ECS MAIN TASK HANDLER
"""
if __name__ == "__main__":
    # Retrieve the command-line arguments
    arguments = sys.argv[1:]

    if not arguments:
        logger.error("No task name provided. Usage: python main.py <task_name> <args_as_json>")
        sys.exit(1)

    task_name = arguments[0]
    task_args = json.loads(arguments[1]) if len(arguments) > 1 else None

    task_mappings = {
        'batch_loader_task': batch_loader_task,
        'check_status_task': check_status_task
    }
    task_func = task_mappings.get(task_name)

    if not task_func:
        logger.error(f"Invalid task name: {task_name}")
        sys.exit(1)

    if task_args is not None:
        task_func(task_args)
    else:
        task_func()
