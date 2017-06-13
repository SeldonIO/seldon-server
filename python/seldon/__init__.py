import logging

from .util import RecommenderWrapper,Recommender,Extension,ExtensionWrapper,Recommender_wrapper,Extension_wrapper

__version__ = '2.2.4'

class NullHandler(logging.Handler):
    def emit(self, record):
        pass

logger = logging.getLogger(__name__)
if len(logger.handlers) == 0:# To ensure reload() doesn't add another one
    logger.addHandler(NullHandler())
