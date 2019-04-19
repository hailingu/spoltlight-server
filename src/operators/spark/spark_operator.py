from abc import abstractmethod

from operators.operator import Operator
from operators.operator_status import OperatorStatus


class SparkOperator(Operator):
    '''Spark backend basic operator'''

    OP_NAME = 'Spark Base Operator'
    OP_CATEGORY = 'Root'

    def __init__(self):
        self.op_input_num = None
        self.op_output_num = None
        self.op_input_ops = []
        self.op_input_ops_index = []
        self.op_result = []
        self.op_status = None
        self.op_json_param = None
        self.op_running_id = None
        self.op_running_mode = None
        self.op_working_directory = None
        self.op_script_location = None
        self.op_backend = 'spark'

    @abstractmethod
    def init_operator(self, op_json_param):
        pass

    def run(self):
        if self.op_running_mode == 'function':
            return self.run_function_mode()
        else:
            return self.run_script_mode()

    @abstractmethod
    def run_script_mode(self):
        pass

    @abstractmethod
    def run_function_mode(self):
        pass

    def get_result(self, index=0):
        return self.op_result[index]

    def get_status(self):
        return self.op_status
