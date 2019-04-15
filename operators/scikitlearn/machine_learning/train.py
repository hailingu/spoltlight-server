import pandas as pd

from sklearn.ensemble import RandomForestClassifier

from operators.scikitlearn.scikitlearn_operator import ScikitlearnOperator
from operators.operator_status import OperatorStatus


class Train(ScikitlearnOperator):
    '''Train model operator '''

    OP_NAME = 'train'
    OP_CATEGORY = 'machine-learning'


    def __init__(self):
        super.__init__()
        self.op_input_num = 2
        self.op_output_num = 1
        self.op_status = OperatorStatus.INIT
        self.label_column = None
        self.train_columns = None
        self.model = None


    def init_operator(self, op_json_param):
        self.op_json_param = op_json_param
        self.op_input_ops = op_json_param['input-ops']
        self.op_input_ops_index = op_json_param['input-ops-index']
        self.label_column = op_json_param['label_column']
        self.train_columns = op_json_param['train_columns']
        self.model = self.op_input_ops[self.op_input_ops_index[0]]

 
    def run(self):
        try:
            data = self.op_input_ops[self.op_input_ops_index[1]]
            x = pd.DataFrame(data, cols=self.train_columns)
            Y = pd.DataFrame(data, cols=self.label_column)
            self.model.fit(x, Y)
            self.op_result.append(self.model)
        except Exception as e:
            print('Exception ' + str(e))
            self.status = OperatorStatus.FAILED

        return self.op_status


    def __str__(self):
        return 'random forest operator'


    def to_string(self):
        return self.__str__()
