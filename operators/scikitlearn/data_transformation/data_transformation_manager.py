from operators.operator_manager import OperatorManager

from operators.scikitlearn.data_transformation.remove_duplicated_rows import RemoveDuplicatedRows

class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        else:
            cls._instances[cls].__init__(*args, **kwargs)
    
        return cls._instances[cls]


class DataTransformationOperatorManager(OperatorManager, metaclass=Singleton):
    '''A data transformation operator manager'''

    MANAGER_NAME = 'data-transformation'

    def __init__(self):
        self.operators = {}

    def register_manager(self, manager):
        return None

    def register_operator(self, operator_class):
        self.operators[operator_class.op_name, operator_class]

    def get_manager(self, manager_name):
        return None

    def get_operator(self, op_name):
        return self.operators[op_name]

dataTransformationOperatorManager = DataTransformationOperatorManager()
dataTransformationOperatorManager.register_operator(RemoveDuplicatedRows.OP_NAME, RemoveDuplicatedRows)
