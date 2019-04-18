from flow.flow import Flow
from flow.flow_status import FlowStatus
from id_generator import idGenerator


class SparkFlow(Flow):
    '''Spotlight spark flow'''

    def __init__(self):
        self.flow_pending_operators = {}
        self.flow_running_operators = {}
        self.flow_success_operators = {}
        self.flow_failded_operators = {}
        self.flow_flow_json = None
        self.flow_id = None
        self.flow_status = FlowStatus.INIT
        self.flow_run_mode = 'script'
        self.flow_scheduler = 'default'

    def init(self, flow_json):
        self.flow_flow_json = flow_json
        self.flow_id = idGenerator()
        self.flow_pending_operators = self.__flow_parser__()
        self.flow_run_mode = self.flow_flow_json['run-mode'] if 'run-mode' in self.flow_flow_json else 'script'

    def run(self):
        if self.flow_run_mode == 'default':
            return self.script_mode()
        elif self.flow_run_mode == 'function-mode':
            return self.function_mode()
        else:
            return self.flow_status

    def script_mode(self):
        pass

    def function_mode(self):
        pass
    
    def __flow_parser__(self):
        operator_processed_list = {}
        return operator_processed_list
