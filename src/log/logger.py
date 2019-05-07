import logging

class Logger:
    '''spotlight logger'''


    FORMAT = '%(asctime)s %(filename)s [line:%(lineno)d] %(levelname)s %(message)s'

    def __init__(self, file_name):
        self.logger = logging.getLogger(file_name)
        self.logger.setLevel(logging.INFO)
        self.handler = logging.FileHandler(file_name + '.log')
        formatter = logging.Formatter(Logger.FORMAT)
        self.handler.setFormatter = formatter
        self.logger.addHandler(self.handler)

    def info(self, msg):
        self.logger.info(msg)

    def debug(self, msg):
        self.logger.debug(msg)

    def warn(self, msg):
        self.logger.warn(msg)