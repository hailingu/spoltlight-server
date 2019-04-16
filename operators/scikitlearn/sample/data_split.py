import pandas as pd

from operators.scikitlearn.scikitlearn_operator import ScikitlearnOperator
from operators.operator_status import OperatorStatus


class DataSplit(ScikitlearnOperator):
    '''data split operator'''

    OP_NAME = 'data-split'
    OP_CATEGORY = 'sample'

    def __init__(self):
        super.__init__()
        self.op_input_num = 1
        self.op_output_num = 2
        self.op_status = OperatorStatus.INIT
        self.percentage = 0.8

    def init_operator(self, op_json_param):
        self.op_json_param = op_json_param
        self.op_input_ops = op_json_param['input-ops']
        self.op_input_ops_index = op_json_param['input-ops-index']
        self.percentage = float(op_json_param['percentage']) if 'percentage' in op_json_param else 0.8

    def run(self):
        try:
            data = self.op_input_ops[0](self.op_input_ops_index[0])
            split_index = int(self.percentage * data.size)
            self.op_result.append(data.iloc[:,:split_index])
            self.op_result.append(data.iloc[:,split_index:])
            self.op_status = OperatorStatus.SUCCESS
        except Exception as e:
            print('Exception ' + str(e))
            self.status = OperatorStatus.FAILED

        return self.op_status

    def __str__(self):
        return 'data split operator operator, percetage is: ' + str(self.percentage)

    def to_string(self):
        return self.__str__()
