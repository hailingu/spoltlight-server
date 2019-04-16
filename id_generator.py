import time


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        else:
            cls._instances[cls].__init__(*args, **kwargs)
    
        return cls._instances[cls]


class IdGenerator(metaclass=Singleton):
    '''spotlight id generator class'''

    # _instance = {}
    
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