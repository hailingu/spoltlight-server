from flow.flow import Flow
from flow.flow_status import FlowStatus
from id_generator import idGenerator
from operators.operator_status import OperatorStatus


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
        while len(self.flow_pending_operators) > 0:
            for op_index in self.flow_pending_operators:
                operator = self.flow_pending_operators[op_index]
                dependency_ready = True
    
                for input_op in operator.op_input_ops:
                    dependency_ready = dependency_ready and (input_op.op_json_param['op-index'] in self.flow_success_operators)

                if dependency_ready:
                    self.flow_running_operators[op_index] = operator
                    status = operator.run_script_mode()

                if status == OperatorStatus.SUCCESS:
                    self.flow_running_operators.pop(op_index)
                    self.flow_success_operators[op_index] = operator
                    self.flow_pending_operators.pop(op_index)

                if status == OperatorStatus.FAILED:
                    self.flow_running_operators.pop(op_index)
                    self.flow_failded_operators[op_index] = operator
                    self.flow_pending_operators.pop(op_index)
                break
                
            if status == None or len(self.flow_failded_operators) > 0:
                self.flow_status = FlowStatus.FAILED
                break

        self.flow_status = FlowStatus.SUCCESS
        return self.flow_status    
            
    def function_mode(self):
        self.flow_status = FlowStatus.SUCCESS
        return self.flow_status
    
    def __flow_parser__(self):
        operator_processed_list = {}
        return operator_processed_list
