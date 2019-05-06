import time

def mkdir(path):
    import os
    path = path.strip()
    path = path.rstrip("\\")
    existed = os.path.exists(path)
    if not existed:
        os.makedirs(path)
        return True
    else:
        return False

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

    def __init__(self):
        self.index = 0
        self.today = time.strftime('%Y-%m-%d',time.localtime(time.time()))

    def __call__(self, *args, **kwargs):
        return self.flow_id_generator()

    def flow_id_generator(self):
        date = time.strftime('%Y-%m-%d',time.localtime(time.time()))
        if not date == self.today:
            self.today = date
            self.index = 0
        self.index = self.index + 1
        return 'spotlight-flow-' + str(int(time.time()) * 1000) + "-" + str(self.index)

    def operator_running_id_generator(self, flow_id, op_index):
        return 'spotlight-flow-' + flow_id + '-' + op_index

idGenerator = IdGenerator()
