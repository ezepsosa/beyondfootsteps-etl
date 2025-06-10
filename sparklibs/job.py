import logging
import argparse
from argparse import ArgumentParser, Namespace
class BaseJob():
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self._config_logger()
    
    def _config_args(self, parser: ArgumentParser) -> Namespace:
        return parser.parse_args()
    
    def execute(self):
        parser = argparse.ArgumentParser(description=f'Job: {self.__class__.__name__}')
        args = self._config_args(parser)
        self.logger.info('Setup needed arguments')
        
        self.run(args)
        
    def run(self, args: Namespace):
        Exception('Function not implemented')
        
    @staticmethod
    def _config_logger():
        logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler()]
    )
    
    logger = logging.getLogger('sparklibs')
    logger.setLevel(logging.DEBUG)