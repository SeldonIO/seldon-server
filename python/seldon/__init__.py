import logging

from .util import Recommender_wrapper,Recommender,Extension,Extension_wrapper

class NullHandler(logging.Handler):
    def emit(self, record):
        pass

logger = logging.getLogger('seldon')
if len(logger.handlers) == 0:# To ensure reload() doesn't add another one
    logger.addHandler(NullHandler())
