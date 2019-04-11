from id_generator import idGenerator
from flow import Flow
from operators.operator_status import OperatorStatus
from operators.scikitlearn.scikitlearn_operator_manager import ScikitlearnOperatorManager


class ScikitlearnFlow(Flow):
    '''A scikit learn flow'''

    def __init__(self, flow_json):
        self.pending_operators = {}
        self.running_operators = {}
        self.success_operators = {}
        self.failded_operators = {}
        self.flow_json = flow_json
        self.pending_operators = self.__flow_parser()
        self.id = idGenerator.id_generator()

    def run(self):
        while len(self.pending_operators) > 0:
            status = None
            for op_index in self.pending_operators:
                operator = self.pending_operators[op_index]
                dependency_ready = True
                if  operator.op_category != 'DataImport':
                    for input_op in operator.input_op:
                        dependency_ready = dependency_ready and (input_op.json_param['op_index'] in self.success_operators)

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
                
            if status == None:
                break
            
            if len(self.failded_operators) > 0:
                break
    
        return None

    def __flow_parser(self):
        operator_pending_list = {}
        operator_processed_list = {}

        operators = self.flow_json['flow']['operators']

        for operator in operators:
            operator_pending_list[operator['op-index']] = operator

        while len(operator_pending_list) > 0:
            for op_index in operator_pending_list:
                operator = operator_pending_list[op_index]
                if op_index in operator_processed_list:
                    continue

                deps_len = len(operator['deps'])
                if deps_len == 0:
                    operator_processed_list[op_index] = ScikitlearnOperatorManager.get_operator(operator['op-name'], operator['op-category'], operator['params'])
                    operator_processed_list[op_index].json_param['op_index'] = op_index
                    operator_pending_list.pop(op_index)
                else:
                    operator['params']['input_op'] = []
                    dependency_ready = True
                    for dep in operator['deps']:
                        dependency_ready = dependency_ready and (dep in operator_processed_list)

                    if not dependency_ready:
                        break

                    for dep in operator['deps']:
                        operator['params']['input_op'].append(operator_processed_list[dep])
                    
                    operator_processed_list[op_index] = ScikitlearnOperatorManager.get_operator(operator['op-name'], operator['op-category'], operator['params'])
                    operator_processed_list[op_index].json_param['op_index'] = op_index
                    operator_pending_list.pop(op_index)    
                break

        return operator_processed_list