from operators.operator_manager import OperatorManager

from operators.scikitlearn.data_import.data_import_manager import dataImportOperatorManager
from operators.scikitlearn.data_transformation.data_transformation_manager import dataTransformationOperatorManager
from operators.scikitlearn.machine_learning.machine_learning_manager import machineLearningOperatorManager


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        else:
            cls._instances[cls].__init__(*args, **kwargs)
    
        return cls._instances[cls]
        

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

scikitlearnOperatorManager = ScikitlearnOperatorManager()
scikitlearnOperatorManager.register_manager(dataImportOperatorManager)
scikitlearnOperatorManager.register_manager(dataTransformationOperatorManager)
scikitlearnOperatorManager.register_manager(machineLearningOperatorManager)
