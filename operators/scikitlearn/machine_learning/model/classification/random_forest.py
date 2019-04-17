import pandas as pd

from sklearn.ensemble import RandomForestClassifier

from operators.scikitlearn.scikitlearn_operator import ScikitlearnOperator
from operators.operator_status import OperatorStatus


class RandomForest(ScikitlearnOperator):
    '''random forest model'''

    OP_NAME = 'random-forest'
    OP_CATEGORY = 'machine-learning'

    def __init__(self):
        super(RandomForest, self).__init__()
        self.op_input_num = 0
        self.op_output_num = 1
        self.op_status = OperatorStatus.INIT
        self.n_estimators = 10
        self.criterion = 'gini'
        self.max_features = 'auto'
        self.max_depth = 5
        self.min_samples_split = 16
        self.min_samples_leaf = 8
        self.random_state = 42
        self.bootstrap = True

    def init_operator(self, op_json_param):
        self.op_json_param = op_json_param
        self.n_estimators = int(op_json_param['n_estimators']) if 'n_estimators' in op_json_param else 10
        self.criterion = op_json_param['criterion'] if 'criterion' in op_json_param else 'gini'
        self.max_features = int(
            op_json_param['max_features']) if 'max_features' in op_json_param else 'auto'
        self.max_depth = int(
            op_json_param['max_depth']) if 'max_depth' in op_json_param else 5
        self.min_samples_split = int(
            op_json_param['min_samples_split']) if 'min_samples_split' in op_json_param else 16
        self.min_samples_leaf = int(
            op_json_param['min_samples_leaf']) if 'min_samples_leaf' in op_json_param else 8
        self.random_state = int(
            op_json_param['random_state']) if 'random_state' in op_json_param else 42
        self.bootstrap = op_json_param['bootstrap'] if 'bootstrap' in op_json_param else True

    def run(self):
        model = RandomForestClassifier(bootstrap=self.bootstrap, oob_score=True, random_state=self.random_state, min_samples_leaf=self.min_samples_leaf,
                                       min_samples_split=self.min_samples_split, max_depth=self.max_depth, max_features=self.max_features, criterion=self.criterion, n_estimators=self.n_estimators)
        self.op_result.append(model)
        self.status = OperatorStatus.SUCCESS
        return self.status

    def __str__(self):
        return 'random forest operator'

    def to_string(self):
        return self.__str__()
