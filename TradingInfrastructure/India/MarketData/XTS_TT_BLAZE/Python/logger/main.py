import logging
import asyncio
from datetime import datetime
import aiofiles


class LoggerBase:
    def __init__(self, 
                 logger=None, 
                 log_file=None):
        
        self.log_file = log_file
        self.logger = logger

        if logger is None:
            self.logger = logging.getLogger(self.__class__.__name__)
            self.logger.setLevel(logging.INFO)
            
            stream_handler = logging.StreamHandler()
            stream_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
            self.logger.addHandler(stream_handler)
            
            if log_file:
                file_handler = logging.FileHandler(log_file)
                file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
                self.logger.addHandler(file_handler)
        else:
            self.logger = logger
        
        self.logger.info(f"{self.__class__.__name__} logger initialized")
    
    def log(self, level, message):
        log_method = getattr(self.logger, level.lower(), self.logger.info)
        log_method(message)

        if self.log_file:
            log_message = f"{datetime.now()} - {self.__class__.__name__} - {level.upper()} - {message}\n"
            with open(self.log_file, mode='a') as f:
                f.write(log_message)

    def info(self, message, *args, **kwargs):
        self.log('info', message)

    def warning(self, message, *args, **kwargs):
        self.log('warning', message)

    def error(self, message, *args, **kwargs):
        self.log('error', message)

    def debug(self, message, *args, **kwargs):
        self.log('debug', message)

    def critical(self, message, *args, **kwargs):
        self.log('critical', message)
    
    def FileError(self, message, *args, **kwargs):
        self.log('FileError', message)

    def IndexKeyError(self, message, *args, **kwargs):
        self.log('IndexKeyError', message)

    def ioError(self, message, *args, **kwargs):
        self.log('I/O Error', message)
        
    