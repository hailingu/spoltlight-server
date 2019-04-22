import sys
from random import random
from operator import add

import numpy
from numpy import allclose

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import *
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.sql.types import DoubleType


if __name__ == "__main__":
    """
        Usage: random_forest input
    """
    spark = SparkSession\
        .builder\
        .appName("Random Forest Algorithm")\
        .getOrCreate()

    input_path = sys.argv[1]
    output = sys.argv[2]
    feature_cols = sys.argv[3].columns.strip("'").split(' ')
    label_col = sys.argv[4]

    max_cagegories = int(sys.argv[5])
    num_trees = int(sys.argv[5])
    max_depth = int(sys.argv[6])
    max_bins = int(sys.argv[7])
    min_instance_per_node = int(sys.argv[8])
    criterion = sys.argv[9]
    feature_subset_strategy = sys.argv[10]
    sub_sampling_rate = float(sys.argv[11])
    min_info_gain = float(sys.argv[12])

    data = spark.read.parquet(input_path)

    for feature in feature_cols:
        data = data.withColumn(feature, data[feature].cast(DoubleType()))

    assembler = VectorAssembler(inputCols=feature_cols, outputCol='features')
    data = assembler.transform(data)

    data = data.select(['features', label_col])
    label_indexer = StringIndexer(
        inputCol=label_col, outputCol='label').fit(data)
    data = label_indexer.transform(data)

    data = data.select(['features', 'label'])

    rf = RandomForestClassifier(maxDepth=max_depth, maxBins=max_bins, minInstancesPerNode=min_instance_per_node, minInfoGain=min_info_gain,
                                impurity=criterion, numTrees=num_trees, featureSubsetStrategy=feature_subset_strategy, subsamplingRate=sub_sampling_rate)
    model = rf.fit(data)

    print('feature importance')
    print(model.featureImportances)

    spark.stop()
