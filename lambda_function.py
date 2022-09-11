import logging.config
from LogginConfig import config
import logging

logging.config.dictConfig(config)
logger = logging.getLogger(__name__)


def lambda_handler(event, context):
    try:
        pass
    except Exception as exception:
        logger.error(exception, exc_info=True)
        return {
            "statusCode": 501,
            "body": "Internal Error!"
        }

