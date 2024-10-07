import logging
import sys
from typing import Optional

class Logger:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Logger, cls).__new__(cls)
            cls._instance._logger = logging.getLogger('EnhancedFilestore')
            cls._instance._logger.setLevel(logging.DEBUG)
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            
            # Console handler
            ch = logging.StreamHandler(sys.stdout)
            ch.setLevel(logging.INFO)
            ch.setFormatter(formatter)
            cls._instance._logger.addHandler(ch)
            
            # File handler
            fh = logging.FileHandler('filestore.log')
            fh.setLevel(logging.DEBUG)
            fh.setFormatter(formatter)
            cls._instance._logger.addHandler(fh)
        
        return cls._instance

    @classmethod
    def get_logger(cls):
        return cls()._logger

    @staticmethod
    def debug(msg: str, *args, **kwargs):
        Logger.get_logger().debug(msg, *args, **kwargs)

    @staticmethod
    def info(msg: str, *args, **kwargs):
        Logger.get_logger().info(msg, *args, **kwargs)

    @staticmethod
    def warning(msg: str, *args, **kwargs):
        Logger.get_logger().warning(msg, *args, **kwargs)

    @staticmethod
    def error(msg: str, *args, **kwargs):
        Logger.get_logger().error(msg, *args, **kwargs)

    @staticmethod
    def critical(msg: str, *args, **kwargs):
        Logger.get_logger().critical(msg, *args, **kwargs)

    @staticmethod
    def exception(msg: str, *args, exc_info: bool = True, **kwargs):
        Logger.get_logger().exception(msg, *args, exc_info=exc_info, **kwargs)
