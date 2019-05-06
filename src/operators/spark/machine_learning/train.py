import subprocess

from operators.spark.spark_operator import SparkOperator
from operators.operator_status import OperatorStatus


class Train(SparkOperator):
    '''Train model operator '''

    OP_NAME = 'train'
    OP_CATEGORY = 'machine-learning'

    def __init__(self):
        super(Train, self).__init__()
        self.op_input_num = 2
        self.op_output_num = 1
        self.op_status = OperatorStatus.INIT
        self.op_script_location = 'resources/spark_operators/machine_learning/train.py'
        self.op_backend = 'spark'

        self.model = None
        self.feature_cols = None
        self.label_col = None
       
    def init_operator(self, op_json_param):
        self.op_json_param = op_json_param
        self.op_input_ops = op_json_param['input-ops']
        self.op_input_ops_index = op_json_param['input-ops-index']
        self.op_running_mode = self.op_json_param['running-mode'] if 'running-mode' in self.op_json_param else 'script'
        self.op_local = bool(self.op_json_param['local']) if 'local' in self.op_json_param else True
        self.op_working_directory = self.op_json_param['op-working-directory'] if 'op-working-directory' in self.op_json_param else None

        self.model = self.op_input_ops[0].get_result(self.op_input_ops_index[0])
        self.feature_cols = self.op_json_param['feature']
        self.label_col = self.op_json_param['label']

    def run_function_mode(self):
        return self.op_status

    def run_script_mode(self):
        run_command = 'spark-submit --master '
        if self.op_local:
            run_command = run_command + 'local[2] ' 

        self.op_result.append(self.op_working_directory + 'output/' + self.op_json_param['op-index'] + '-output')
        
        input_path = self.op_input_ops[1].get_result(self.op_input_ops_index[1])

        run_command = None
        if self.model.OP_NAME == 'random-forest':
            self.model.run_script_mode()
            run_command = run_command + self.model.op_script_location + input_path + ' ' + self.op_result[0] + ' ' + self.feature_cols + ' ' + self.label_col +  ' ' + self.model.run_args

        sub_proc = None
        if run_command != None:
            sub_proc = subprocess.Popen(run_command, stdout=subprocess.PIPE, shell=True)
            sub_proc.wait()
            return sub_proc.returncode

        return None

    def azkaban_script(self):
        run_command = 'spark-submit --master '
        if self.op_local:
            run_command = run_command + 'local[2] '

        self.op_result.append(self.op_working_directory + 'output/' + self.op_json_param['op-index'] + '-output')
        input_path = self.op_input_ops[1].get_result(self.op_input_ops_index[1])

        run_command = None
        if self.model.OP_NAME == 'random-forest':
            self.model.run_script_mode()
            run_command = run_command + self.model.op_script_location + input_path + ' ' + self.op_result[0] + ' ' + self.feature_cols + ' ' + self.label_col +  ' ' + self.model.run_args

        return run_command    
    