import os

from operators.spark.spark_operator import SparkOperator
from operators.operator_status import OperatorStatus
from id_generator import idGenerator

import subprocess


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
        self.op_local = True
        self.op_script_location = 'resources/spark_operators/data_iport/import_csv.py'
        self.op_backend = 'spark'

    def init(self, op_json_param):
        self.op_json_param = op_json_param
        self.input_path = self.op_json_param['input-path']
        self.delimiter = self.op_json_param['delimiter'] if 'delimiter' in self.op_json_param else ','
        self.op_running_mode = self.op_json_param['running-mode'] if 'running-mode' in self.op_json_param else 'script'
        self.op_local = bool(self.op_json_param['local']) if 'local' in self.op_json_param else True
        self.op_working_directory = self.op_json_param['op-working-directory'] if 'op-working-directory' in self.op_json_param else None 
        
    def run_function_mode(self):
        return self.op_status

    def run_script_mode(self):
        run_command = 'spark-submit --master '
        if self.op_local:
            run_command = run_command + 'local[2] ' 

        self.op_result.append(self.op_working_directory + 'output/' + self.op_json_param['op-index'] + '-output')
        run_command = self.op_script_location + ' ' + self.input_path + ' ' + self.op_result[0] + ' ' + self.delimiter
        sub_proc = subprocess.Popen(run_command, stdout=subprocess.PIPE, shell=True)
        sub_proc.wait(20)
        return sub_proc.returncode

