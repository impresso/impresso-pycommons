import pathlib
import logging
from contextlib import ExitStack

from impresso_commons.utils.utils import get_pkg_resource

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# suppressing botocore's verbose logging
logging.getLogger('botocore').setLevel(logging.WARNING)
logging.getLogger('smart_open').setLevel(logging.WARNING)

file_mng = ExitStack()
log_dir = get_pkg_resource(file_mng, 'data/logs/')
log_file = get_pkg_resource(
    file_mng,
    'data/logs/tests.log'
)
pathlib.Path(log_dir).mkdir(parents=True, exist_ok=True)

handler = logging.FileHandler(filename=log_file, mode='w')
formatter = logging.Formatter(
    '%(asctime)s %(name)-12s %(levelname)-8s %(message)s'
)
handler.setFormatter(formatter)
logger.addHandler(handler)

S3_CANONICAL_BUCKET = "s3://canonical-data"
S3_REBUILT_BUCKET = "s3://rebuilt-data"

file_mng.close()