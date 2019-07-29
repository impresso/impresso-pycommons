import logging
from impresso_commons.utils import init_logger


def test_logger():
    logger = init_logger(logging.getLogger(), logging.DEBUG, None)
    logger.info(f"CLI arguments received: {logger}")