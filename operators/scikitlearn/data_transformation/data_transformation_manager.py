from operators.operator_manager import OperatorManager
from utils.utils import Singleton

from operators.scikitlearn.data_transformation.remove_duplicated_rows import RemoveDuplicatedRows

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
