from abc import abstractmethod, ABC

class Operator(ABC):
    '''A splotlight base operator class'''

    @abstractmethod
    def run(self):
        pass

    @abstractmethod
    def get_status(self):
        pass