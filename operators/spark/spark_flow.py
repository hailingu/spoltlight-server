from flow import Flow
from id_generator import idGenerator
from utils.utils import mkdir
import os

class SparkFlow(Flow):
    '''A spark flow, it use a azkaban as schedule.'''

    def __init__(self, flow_json):
        self.flow_json = flow_json
        self.backend = 'spark'
        self.scheduler = 'azkaban'
        self.id = idGenerator.id_generator()

    def generate(self):
        if self.scheduler == 'azkaban':
            operators = self.flow_json['flow']['operators']
            mkdir(self.id)
            os.system('cp resources/azkaban_job_template/flow20.project ' + str(self.id))
            for operator in operators:
                if operator['name'] == 'IrisMultiClassData':
                    return None
                elif operator['name'] == 'RemoveDuplicatedRows':
                    os.system('cp operators/spark/data_transformation/remove_duplicated_rows.py ' + str(self.id))

            return None            



        return None