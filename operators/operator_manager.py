from abc import abstractmethod, ABC

class OperatorManager(ABC):
    '''Operator manager base class'''

    @abstractmethod
    def register_manager(self, manager):
        pass

    @abstractmethod
    def register_operator(self, operator):
        pass

    @abstractmethod
    def get_manager(self, manager_name):
        pass

    @abstractmethod
    def get_operator(self, op_name):
        pass