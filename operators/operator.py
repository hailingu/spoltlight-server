from abc import abstractmethod, ABC

class Operator(ABC):
    '''A splotlight base operator class'''

    @abstractmethod
    def get_status(self):
        pass