from operators.operator_status import OperatorStatus
from operators.scikitlearn.scikitlearn_operator import ScikitlearnOperator


class RemoveDuplicatedRows(ScikitlearnOperator):
    '''Remove duplicated rows from data with selected columns'''

    OP_NAME = 'remove-duplicated-rows'
    OP_CATEGORY = 'data-transformation'

    def __init__(self):
        super(RemoveDuplicatedRows, self).__init__()
        self.op_input_num = 1
        self.op_output_num = 1
        self.op_status = OperatorStatus.INIT
        self.columns = None
       
    def init_operator(self, op_json_param):
        self.op_json_param = op_json_param
        self.op_input_ops = op_json_param['input-ops']
        self.op_input_ops_index = op_json_param['input-ops-index']
        self.columns = op_json_param['columns'] if 'columns' in op_json_param else None

    def run(self):
        try:
            data = self.op_input_ops[0].get_result(index=self.op_input_ops_index[0])
            data.drop_duplicates(subset=self.columns.strip("'").split(' '), inplace=True)
            self.op_result.append(data)
            self.status = OperatorStatus.SUCCESS
        except Exception as e:
            print('Exception ' + str(e))
            self.status = OperatorStatus.FAILED
        return self.status
    
    def __str__(self):
        return 'remove duplicated rows operator, selected columns: ' + self.columns

    def to_string(self):
        return self.__str__()