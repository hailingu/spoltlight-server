import subprocess

from operators.operator_status import OperatorStatus
from operators.spark.spark_operator import SparkOperator


class RemoveDuplicatedRows(SparkOperator):
    '''Remove duplicated rows from data with selected columns'''

    OP_NAME = 'remove-duplicated-rows'
    OP_CATEGORY = 'data-transformation'

    def __init__(self):
        super(RemoveDuplicatedRows, self).__init__()
        self.op_input_num = 1
        self.op_output_num = 1
        self.op_status = OperatorStatus.INIT
        self.op_script_location = 'resources/spark_operators/data_iport/remove_duplicated_rows.py'
        self.op_backend = 'spark'

        self.columns = None
       
    def init_operator(self, op_json_param):
        self.op_json_param = op_json_param
        self.op_input_ops = op_json_param['input-ops']
        self.op_input_ops_index = op_json_param['input-ops-index']
        self.op_running_mode = self.op_json_param['running-mode'] if 'running-mode' in self.op_json_param else 'script'
        self.op_local = bool(self.op_json_param['local']) if 'local' in self.op_json_param else True
        self.op_working_directory = self.op_json_param['op-working-directory'] if 'op-working-directory' in self.op_json_param else None

        self.columns = op_json_param['columns'] if 'columns' in op_json_param else None

    def run_function_mode(self):
        return self.op_status

    def run_script_mode(self):
        run_command = 'spark-submit --master '
        if self.op_local:
            run_command = run_command + 'local[2] ' 

        self.op_result.append(self.op_working_directory + 'output/' + self.op_json_param['op-index'] + '-output')
        run_command = run_command + self.op_script_location + ' ' + self.op_input_ops[0].get_result(self.op_input_ops_index[0]) + ' ' + self.op_result[0] + ' ' + self.columns
        sub_proc = subprocess.Popen(run_command, stdout=subprocess.PIPE, shell=True)
        sub_proc.wait(20)
        return sub_proc.returncode