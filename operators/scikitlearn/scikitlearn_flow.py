from id_generator import idGenerator
from flow.flow import Flow
from flow.flow_status import FlowStatus
from operators.operator_status import OperatorStatus
from operators.scikitlearn.scikitlearn_operator_manager import scikitlearnOperatorManager


class ScikitlearnFlow(Flow):
    '''A scikit learn flow'''

    def __init__(self):
        self.flow_pending_operators = {}
        self.flow_running_operators = {}
        self.flow_success_operators = {}
        self.flow_failded_operators = {}
        self.flow_json = None
        self.flow_id = None
        self.flow_status = FlowStatus.INIT
        self.flow_scheduler = 'default'
        
    def init(self, flow_json):
        self.flow_json = flow_json
        self.flow_pending_operators = self.__flow_parser__()
        self.flow_id = idGenerator()

    def run(self):
        while len(self.flow_pending_operators) > 0:
            status = None
            for op_index in self.flow_pending_operators:
                operator = self.flow_pending_operators[op_index]
                dependency_ready = True
    
                for input_op in operator.op_input_ops:
                    dependency_ready = dependency_ready and (input_op.op_json_param['op-index'] in self.flow_success_operators)

                if dependency_ready:
                    self.flow_running_operators[op_index] = operator
                    status = operator.run()

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

    def __flow_parser__(self):
        operator_pending_list = []
        operator_processed_list = {}

        operators = self.flow_json['flow']['operators']
        for operator in operators:
            operator_pending_list.append(operator)

        while len(operator_pending_list) > 0:
            for op in operator_pending_list:
                operator = op
                op_index = operator['op-index']

                if operator['op-index'] in operator_processed_list:
                    continue
                
                deps_len = len(operator['deps'])
                if deps_len == 0:
                    operator['params']['op-index'] = operator['op-index']
                    operator_manager = scikitlearnOperatorManager.get_manager(operator['op-category'])
                    scikitlearn_operator = operator_manager.get_operator(operator['op-name'])()
                    scikitlearn_operator.init_operator(operator['params'])
                    scikitlearn_operator.op_running_id = idGenerator.operator_running_id_generator()
                    operator_processed_list[op_index] = scikitlearn_operator
                    operator_pending_list.remove(operator)
                else:
                    operator['params']['input-ops'] = []
                    dependency_ready = True

                    for dep in operator['deps']:
                        dependency_ready = dependency_ready and (dep['op-index'] in operator_processed_list)

                    if not dependency_ready:
                        operator_pending_list.pop(0)
                        operator_pending_list.append(operator)
                        
                        break

                    for dep in operator['deps']:
                        operator['params']['input-ops'].append(operator_processed_list[dep['op-index']])
                        if not 'input-ops-index' in operator['params']:
                            operator['params']['input-ops-index'] = []
                        operator['params']['input-ops-index'].append(dep['op-out-index'])

                    operator['params']['op-index'] = operator['op-index']
                    operator_manager = scikitlearnOperatorManager.get_manager(operator['op-category'])
                    scikitlearn_operator = operator_manager.get_operator(operator['op-name'])()
                    scikitlearn_operator.op_running_id = idGenerator.operator_running_id_generator()
                    scikitlearn_operator.init_operator(operator['params'])
                    operator_processed_list[op_index] = scikitlearn_operator
                    operator_pending_list.remove(operator)
                break

        return operator_processed_list
