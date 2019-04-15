from operators.operator_manager import OperatorManager
from utils.utils import Singleton

from operators.scikitlearn.machine_learning.model.classification.random_forest import RandomForest

class MachineLearningOperatorManager(OperatorManager):
    '''A machine learning operator manager'''

    MANAGER_NAME = 'machine-learning'

    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, '_instance'):
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        self.operators = {}

    def register_manager(self, manager):
        return None

    def register_operator(self, operator_class):
        self.operators[operator_class.OP_NAME] = operator_class

    def get_manager(self, manager_name):
        return None

    def get_operator(self, op_name):
        return self.operators[op_name]

machineLearningOperatorManager = MachineLearningOperatorManager()
machineLearningOperatorManager.register_operator(RandomForest)
