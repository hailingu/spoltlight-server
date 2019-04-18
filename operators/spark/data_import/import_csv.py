import os

from operators.spark.spark_operator import SparkOperator
from operators.operator_status import OperatorStatus
from id_generator import idGenerator



class ImportCSV(SparkOperator):

    '''import csv formate data'''

    OP_NAME = 'import-csv'
    OP_CATEGORY = 'data-import'

    def __init__(self):
        super(ImportCSV, self).__init__()
        self.op_input_num = 0
        self.op_output_num = 1
        self.op_status = OperatorStatus.INIT
        self.op_json_param = None
        self.op_running_id = None
        self.op_running_mode = None
        self.op_script_location = 'resources/spark_operators/data_iport/import_csv.py'
        self.op_backend = 'spark'

    def init(self, op_json_param):
        self.op_json_param = op_json_param
        self.input_path = self.op_json_param['input-path']
        self.delimiter = self.op_json_param['delimiter'] if 'delimiter' in self.op_json_param else ','
        self.op_running_mode = self.op_json_param['running-mode'] if 'running-mode' in self.op_json_param else 'script'
        self.op_running_command = self.op_json_param['running-command'] if 'running-command' in self.op_json_param else 'spark-submit --master local[2]'
        
    def run(self):
        if self.op_running_mode == 'function':
            return self.run_function_mode()
        else:
            return self.run_script_mode()
        
    def run_function_mode(self):
        return self.op_status

    def run_script_mode(self):
        run_script =  'import_csv.py ' + self.input_path + ' ' + self.op_result[0] + ' ' + self.delimiter
        return os.system(self.op_running_command + ' ' + run_script)
        
