import pandas as pd

from operators.scikitlearn.scikitlearn_operator import ScikitlearnOperator
from operators.operator_status import OperatorStatus


class ImportCSV(ScikitlearnOperator):
    '''Import data in csv format'''

    OP_NAME = 'import-csv'
    OP_CATEGORY = 'data-import'

    def __init__(self):
        super(ImportCSV, self).__init__()
        self.op_input_num = 0
        self.op_output_num = 1
        self.op_status = OperatorStatus.INIT
        self.input_path = None
        self.seperator = None

    def init_operator(self, op_json_param):
        self.op_json_param = op_json_param
        self.input_path = self.op_json_param['input-path']
        self.seperator = self.op_json_param['seperator'] if 'seperator' in self.op_json_param else ','

    def run(self):
        try:
            self.op_result.append(pd.read_csv(self.input_path, header=0, sep=self.seperator))
            self.status = OperatorStatus.SUCCESS
        except Exception as e:
            print('Exception ' + str(e))
            self.status = OperatorStatus.FAILED
        return self.status

    def __str__(self):
        return 'import csv data operator, file location: ' + self.input_path

    def to_string(self):
        return self.__str__()

    