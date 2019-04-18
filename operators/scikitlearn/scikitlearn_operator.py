from abc import abstractmethod

from operators.operator import Operator
from operators.operator_status import OperatorStatus


class ScikitlearnOperator(Operator):
    '''Scikitlearn basic operator'''

    OP_NAME = 'Scikitlearn Base Operator'
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
        self.op_backend = 'scikit-learn'

    @abstractmethod
    def init_operator(self, op_json_param):
        pass

    @abstractmethod
    def run(self):
        pass

    def get_result(self, index=0):
        return self.op_result[index]

    def get_status(self):
        return self.op_status