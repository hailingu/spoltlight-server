import time

from utils.utils import Singleton

class IdGenerator(metaclass=Singleton):
    '''spotlight id generator class'''
    
    def __init__(self):
        self.index = 0
        self.today = time.strftime('%Y-%m-%d',time.localtime(time.time()))

    def id_generator(self):
        date = time.strftime('%Y-%m-%d',time.localtime(time.time()))
        if not date == self.today:
            self.today = date
            self.index = 0
        self.index = self.index + 1
        return 'spotlight-flow-' + str(int(time.time()) * 1000) + "-" + str(self.index)

    def __call__(self, *args, **kwargs):
        return self.id_generator()

idGenerator = IdGenerator()