from abc import ABC


class Task(ABC):
    '''base task class in spotlight'''
    
    pass


class FlowTask(Task):
    '''flow task in spotlight'''

    def __init__(self, flow):
        self.flow = flow
        self.proc = None

    def set_proc(self, proc):
        self.proc = proc