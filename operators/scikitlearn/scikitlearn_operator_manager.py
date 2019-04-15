from operators.operator_manager import OperatorManager
from utils.utils import Singleton

from operators.scikitlearn.data_import.data_import_manager import dataImportOperatorManager
from operators.scikitlearn.data_transformation.data_transformation_manager import dataTransformationOperatorManager
from operators.scikitlearn.machine_learning.machine_learning_manager import machineLearningOperatorManager

class ScikitlearnOperatorManager(OperatorManager, metaclass=Singleton):
    '''A scikit learn operator manager'''

    def __init__(self):
        self.managers = {}

    def register_manager(self, manager):
        self.managers[manager.MANAGER_NAME] = manager

    def register_operator(self, operator_class):
        return None

    def get_manager(self, manager_name):
        return self.managers[manager_name]

    def get_operator(self, op_name):
        return None

ScikitlearnOperatorManager = ScikitlearnOperatorManager()
ScikitlearnOperatorManager.register_manager(dataImportOperatorManager)
ScikitlearnOperatorManager.register_manager(dataTransformationOperatorManager)
ScikitlearnOperatorManager.register_manager(machineLearningOperatorManager)
