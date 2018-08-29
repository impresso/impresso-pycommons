import logging
import pkg_resources

logger = logging.getLogger('impresso_commons')
logger.setLevel(logging.INFO)

# suppressing botocore's verbose logging
logging.getLogger('botocore').setLevel(logging.WARNING)

log_file = pkg_resources.resource_filename(
    'impresso_commons',
    'data/logs/tests.log'
)
handler = logging.FileHandler(filename=log_file, mode='w')
formatter = logging.Formatter(
    '%(asctime)s %(name)-12s %(levelname)-8s %(message)s'
)
handler.setFormatter(formatter)
logger.addHandler(handler)
