import logging
from logging import StreamHandler, Formatter
import sys
from config import SERVICE_NAME


class Logger_base:
    def __init__(self, level='DEBUG'):
        self.logger = logging.getLogger(__name__)
        self.handler = StreamHandler(stream=sys.stdout)
        self.level = level

    def logger_configuration(self):
        self.logger.setLevel(self.level)
        self.handler.setFormatter(Formatter(fmt='[%(asctime)s: %(levelname)s] %(message)s'))
        self.logger.addHandler(self.handler)
        return self.logger


class Logger_more_information(Logger_base):
    def logger_configuration(self, extra=None):
        self.logger.setLevel(self.level)
        self.handler.setFormatter(Formatter(fmt=f'[%(asctime)s: %(levelname)s] %(message)s %({extra})s'))
        self.logger.addHandler(self.handler)
        self.logger.addHandler(Myhandler())
        return self.logger


class Myhandler(logging.Handler):
    def emit(self, record):
        from rabbitmq_ import Rabbit_sender
        reserved_keys = record.__dict__
        self.response_string = f"{SERVICE_NAME}: [{reserved_keys['asctime']}: {reserved_keys['levelname']}]{reserved_keys['msg']} ------ {reserved_keys[extra_name]}"
        Rabbit_sender(message=self.response_string).send_message()


extra_name = 'more_informations'
loggers = Logger_more_information(level='INFO').logger_configuration(extra=extra_name)
LOGGER = loggers
