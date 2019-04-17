from id_generator import idGenerator
from flow import Flow
from operators.operator_status import OperatorStatus
from operators.scikitlearn.scikitlearn_operator_manager import scikitlearnOperatorManager


class ScikitlearnFlow(Flow):
    '''A scikit learn flow'''

    def __init__(self, flow_json):
        self.pending_operators = {}
        self.running_operators = {}
        self.success_operators = {}
        self.failded_operators = {}
        self.flow_json = flow_json
        self.pending_operators = self.__flow_parser__()
        self.id = idGenerator()

    def run(self):
        while len(self.pending_operators) > 0:
            status = None
            for op_index in self.pending_operators:
                operator = self.pending_operators[op_index]
                dependency_ready = True
    
                for input_op in operator.op_input_ops:
                    dependency_ready = dependency_ready and (input_op.op_json_param['op-index'] in self.success_operators)

                if dependency_ready:
                    self.running_operators[op_index] = operator
                    status = operator.run()

                if status == OperatorStatus.SUCCESS:
                    self.running_operators.pop(op_index)
                    self.success_operators[op_index] = operator
                    self.pending_operators.pop(op_index)

                if status == OperatorStatus.FAILED:
                    self.running_operators.pop(op_index)
                    self.failded_operators[op_index] = operator
                    self.pending_operators.pop(op_index)
                break
                
            if status == None or len(self.failded_operators) > 0:
                break

        return None

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
