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
        self.id = idGenerator.id_generator()
        self.working_dir = os.getcwd() + '/' + self.id + '/'
        self.__op_output_list = {}
        self.operators = self.__flow_parser()

    def run(self):
        self.init_azkaban_flow()
        self.generate_azkaban_flow()
 
    def init_azkaban_flow(self):
        mkdir(self.working_dir)
        mkdir(self.working_dir + 'azkaban/')
        mkdir(self.working_dir + 'output/')
        os.system('cp resources/azkaban_job_template/flow20.project ' + self.working_dir + 'azkaban/')
        

    def generate_azkaban_flow(self):
        azkaban_flow = AzkabanFlowReadAndWriteHelper(self.working_dir + 'azkaban/' + self.id + '.flow')
        for operator in self.operators:
            os.system('cp ' +  self.operators[operator].spark_operator.script_location + ' ' + self.working_dir + '/azkaban/')
            azkaban_flow.write(self.operators[operator])

        azkaban_flow.close()
        os.system('cd ' + self.working_dir + '/azkaban/&&zip -r ' + self.id + '.zip .&&mv ' + self.id + '.zip ..')
        azkabanClient.login()
        azkabanClient.create_project('splotlight-project', 'spotlight-project')
        azkabanClient.upload(self.working_dir + '/' + self.id, 'splotlight-project')        
        azkabanClient.execute_flow('splotlight-project', self.id)
        
        return self.id

    def __flow_parser(self):
        operator_pending_list = {}
        operator_processed_list = {}
        
        if len(self.__op_output_list) == 0:
            self.__op_output_generator()
        
        if self.scheduler == 'azkaban':
            operators = self.flow_json['flow']['operators']
 

            for operator in operators:
                operator_pending_list[operator['op-index']] = operator

            while len(operator_pending_list) > 0:
                for op_index in operator_pending_list:
                    operator = operator_pending_list[op_index]
                    if op_index in operator_processed_list:
                        continue

                    operator['params']['output'] = self.__op_output_list[op_index]
                    deps_len = len(operator['deps'])
                    if  deps_len > 0:
                        if deps_len == 1:
                            operator['params']['input'] = self.__op_output_list[operator['deps'][0]]  
                        else:
                            i = 1
                            for dep in operator['deps']:
                                operator['params']['input' + str(i)] = self.__op_output_list[dep]
                                i = i + 1
                    op = AzkabanSparkOperator(operator, self.id)
                    operator_processed_list[op_index] = op
                    operator_pending_list.pop(op_index)
                    break

        return operator_processed_list


    def __op_output_generator(self):
        operators = self.flow_json['flow']['operators']
        for operator in operators:
            self.__op_output_list[operator['op-index']] =  self.working_dir + 'output/' + operator['op-index'] + '-output'

               