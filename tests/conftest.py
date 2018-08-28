import sys
import logging

logging.basicConfig(level=logging.INFO, stream=sys.stdout)

# suppressing botocore's verbose logging
logging.getLogger('botocore').setLevel(logging.WARNING)
logger = logging.getLogger('impresso_commons')
