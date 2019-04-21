import subprocess

from operators.spark.spark_operator import SparkOperator
from operators.operator_status import OperatorStatus


class DataSplit(SparkOperator):
    '''data split operator'''

    OP_NAME = 'data-split'
    OP_CATEGORY = 'sample'

    def __init__(self):
        super(DataSplit, self).__init__()
        self.op_input_num = 1
        self.op_output_num = 2
        self.op_status = OperatorStatus.INIT
        self.op_local = True
        self.op_script_location = 'resources/sparkt_perators/sample/data_split.py'
        self.percentage = 0.8

    def init(self, op_json_param):
        self.op_json_param = op_json_param
        self.op_input_ops = self.op_json_param['input-ops']
        self.op_input_ops_index = op_json_param['input-ops-index']
        self.op_running_mode = self.op_json_param['running-mode'] if 'running-mode' in self.op_json_param else 'script'
        self.op_local = bool(self.op_json_param['local']) if 'local' in self.op_json_param else True
        self.op_working_directory = self.op_json_param['op-working-directory'] if 'op-working-directory' in self.op_json_param else None 
        self.percentage = float(op_json_param['percentage']) if 'percentage' in op_json_param else 0.8

    def run_function_mode(self):
        return self.op_status

    def run_script_mode(self):
        run_command = 'spark-submit --master '
        if self.op_local:
            run_command = run_command + 'local[2] ' 

        self.op_result.append(self.op_working_directory + 'output/' + self.op_json_param['op-index'] + '-output')
        run_command = self.op_script_location + ' ' + self.op_input_ops[0].get_result(self.op_input_ops_index[0]) + ' ' + self.op_result[0] + ' ' + self.percentage
        sub_proc = subprocess.Popen(run_command, stdout=subprocess.PIPE, shell=True)
        sub_proc.wait(20)
        return sub_proc.returncode
