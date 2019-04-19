from abc import ABC, abstractmethod
from flow.flow import Flow


class Scheduler(ABC):
    '''Schedule base class'''

    @abstractmethod
    def submit(self, flow):
        pass

    @abstractmethod
    def get_status(self, flow_id):
        pass

    @abstractmethod
    def run(self, flow_id):
        pass

    @abstractmethod
    def pause(self, flow_id):
        pass

    @abstractmethod
    def stop(self, flow_id):
        pass
        