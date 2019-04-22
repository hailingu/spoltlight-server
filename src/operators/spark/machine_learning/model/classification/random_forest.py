import pandas as pd

from operators.spark.spark_operator import SparkOperator
from operators.operator_status import OperatorStatus

from pyspark.ml.classification import RandomForestClassifier


class RandomForest(SparkOperator):
    '''random forest model'''

    OP_NAME = 'random-forest'
    OP_CATEGORY = 'machine-learning'

    def __init__(self):
        super(RandomForest, self).__init__()
        self.op_input_num = 1
        self.op_output_num = 1
        self.op_status = OperatorStatus.INIT
        self.op_script_location = 'resources/spark_operators/machine_learning/model/classification/random_forest.py'
        self.op_backend = 'spark'

        self.max_cagegories = None
        self.num_trees = None
        self.max_depth = None
        self.max_bins = None
        self.min_instance_per_node = None
        self.criterion = None
        self.feature_subset_strategy = None
        self.sub_sampling_rate = None
        self.min_info_gain = None

    def init_operator(self, op_json_param):
        self.op_json_param = op_json_param
        self.op_input_ops = op_json_param['input-ops']
        self.op_input_ops_index = op_json_param['input-ops-index']
        self.op_running_mode = self.op_json_param['running-mode'] if 'running-mode' in self.op_json_param else 'script'
        self.op_local = bool(
            self.op_json_param['local']) if 'local' in self.op_json_param else True
        self.op_working_directory = self.op_json_param[
            'op-working-directory'] if 'op-working-directory' in self.op_json_param else None

        self.max_cagegories = int(
            self.op_json_param['max-cagegories']) if 'max-cagegories' in self.op_json_param else 4
        self.num_trees = int(
            self.op_json_param['num-trees']) if 'num-trees' in self.op_json_param else 10
        self.max_depth = int(
            self.op_json_param['max-depth']) if 'max-depth' in self.op_json_param else 5
        self.max_bins = int(
            self.op_json_param['max-bins']) if 'max-bins' in self.op_json_param else 16
        self.min_instance_per_node = int(
            self.op_json_param['min-instance-per-node']) if 'min-instance-per-node' in self.op_json_param else 8
        self.criterion = self.op_json_param['criterion'] if 'criterion' in self.op_json_param else 'gini'
        self.feature_subset_strategy = self.op_json_param[
            'feature-subset-strategy'] if 'feature-subset-strategy' in self.op_json_param else 'auto'
        self.sub_sampling_rate = float(
            self.op_json_param['sub-sampling-rate']) if 'sub-sampling-rate' in self.op_json_param else 1
        self.min_info_gain = float(
            self.op_json_param['min_info_gain']) if 'min_info_gain' in self.op_json_param else 0.0

    def run_function_mode(self):
        self.op_result.append(RandomForestClassifier(featuresCol='features', labelCol='label', maxDepth=self.max_depth, maxBins=self.max_bins, minInstancesPerNode=self.min_instance_per_node,
                                                     impurity=self.criterion, numTrees=self.num_trees, featureSubsetStrategy=self.feature_subset_strategy, seed=None, subsamplingRate=self.sub_sampling_rate))
        return self.op_status

    def run_script_mode(self):
        return self.op_status