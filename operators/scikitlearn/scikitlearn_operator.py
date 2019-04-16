from abc import abstractmethod

from operators.operator import Operator
from operators.operator_status import OperatorStatus


class ScikitlearnOperator(Operator):
    OP_NAME = 'Scikitlearn Base Operator'
    OP_CATEGORY = 'Root'

    def __init__(self):
        self.op_input_num = None
        self.op_output_num = None
        self.op_input_ops = []
        self.op_input_ops_index = None
        self.op_result = []
        self.op_status = None
        self.op_json_param = None
        self.op_backend = 'scikit-learn'


    @abstractmethod
    def init_operator(self, op_json_param):
        pass

    @abstractmethod
    def run(self):
        pass

    @abstractmethod
    def get_result(self, index=0):
        pass