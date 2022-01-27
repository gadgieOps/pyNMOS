import logging
import logging.config
from logging.handlers import RotatingFileHandler
import os

# Create the Logger
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

if not os.path.exists('/home/dave/.nmos_client/logs'):
    os.makedirs('/home/dave/.nmos_client/logs')


# Create the Handler for logging data to a file
logger_handler = RotatingFileHandler(f'/home/dave/.nmos_client/logs/nmos.log', maxBytes=2000000, backupCount=20)
logger_handler.setLevel(logging.DEBUG)

# Create a Formatter for formatting the log messages
logger_formatter = logging.Formatter('%(name)s - %(levelname)s - %(message)s')

# Add the Formatter to the Handler
logger_handler.setFormatter(logger_formatter)

# Add the Handler to the Logger
log.addHandler(logger_handler)
log.info('============================================================================')
log.info('nmos-client running')
log.info('Logging initialized')
log.info('============================================================================')
