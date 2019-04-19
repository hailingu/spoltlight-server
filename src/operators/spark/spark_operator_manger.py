from operators.operator_manager import OperatorManager


class SparkOperatorManager(OperatorManager):
    '''A spark operator manager'''

    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, '_instance'):
            cls._instance = super().__new__(cls)
        return cls._instance

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

sparkOperatorManager = SparkOperatorManager()