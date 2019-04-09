from flow import Flow
from id_generator import idGenerator
from utils.utils import mkdir
from operators.spark.utils.azkaban_flow_helper import AzkabanFlowReadAndWriteHelper, AzkabanSparkOperator
from operators.spark.utils.azkaban_client import azkabanClient
import os

class SparkFlow(Flow):
    '''A spark flow, it use the azkaban as schedule.'''

    def __init__(self, flow_json):
        self.flow_json = flow_json
        self.backend = 'spark'
        self.scheduler = 'azkaban'
        self.id = str(idGenerator.id_generator())
        self.working_dir = self.id
        self.operators = {}

    def generate(self):
        # if self.scheduler == 'azkaban':
        #     operators = self.flow_json['flow']['operators']
        #     mkdir(self.working_dir)
        #     mkdir(self.working_dir + '/azkaban/')
        #     mkdir(self.working_dir + '/output/')
        #     os.system('cp resources/azkaban_job_template/flow20.project ' + self.working_dir + '/azkaban/')
        #     azkaban_flow = AzkabanFlowReadAndWriteHelper(self.working_dir + '/azkaban/' + self.id + '.flow')
        #     for operator in operators:
        #         if operator['name'] == 'IrisMultiClassData':
        #             op_output = '/home/hailingu/Git/spotlight-server/' + self.working_dir + '/output/' + operator['op_index'] + '_output'
        #             op = AzkabanSparkOperator('data_import.py', operator['params'], operator['op_index'], operator['deps'], op_output)
        #             os.system('cp operators/spark/data_import/data_import.py ' + self.working_dir + '/azkaban/')
        #             azkaban_flow.write(op)
        #             self.operators[operator['op_index']] = op
        #         elif operator['name'] == 'RemoveDuplicatedRows':
        #             op_output = '/home/hailingu/Git/spotlight-server/' + self.working_dir + '/output/' +  operator['op_index'] + '_output'
        #             op = AzkabanSparkOperator('remove_duplicated_rows.py', operator['params'], operator['op_index'], operator['deps'], op_output)
        #             os.system('cp operators/spark/data_transformation/remove_duplicated_rows.py ' + self.working_dir + '/azkaban/')
        #             azkaban_flow.write(op)

        #     azkaban_flow.close()
        #     os.system('cd ' + self.working_dir + '/azkaban/&&zip -r ' + self.id + '.zip .&&mv ' + self.id + '.zip ..')
        #     azkabanClient.login()
        #     azkabanClient.upload(self.working_dir + '/' + self.id, 'test2')
        #     azkabanClient.execute_flow('test2', self.id)
        #     return None
        return None

    def flow_parser(self):
        return None