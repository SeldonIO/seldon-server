import logging

from .util import Recommender_wrapper,Recommender

__version__ = '1.4.0'

class NullHandler(logging.Handler):
    def emit(self, record):
        pass

logger = logging.getLogger('seldon')
if len(logger.handlers) == 0:# To ensure reload() doesn't add another one
    logger.addHandler(NullHandler())
